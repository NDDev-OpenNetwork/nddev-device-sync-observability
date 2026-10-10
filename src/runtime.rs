//! Bounded process, HTTP and delivery adapters; internal Compose network only.
use crate::{
    application::Error,
    config::Config,
    http::{App, router},
    signals::Signals,
};
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
    task::JoinSet,
};

async fn delivery(app: Arc<App>, mut stop: watch::Receiver<bool>) -> Result<(), Error> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(8))
        .connect_timeout(Duration::from_secs(2))
        .redirect(reqwest::redirect::Policy::none())
        .pool_max_idle_per_host(1)
        .build()
        .map_err(|_| Error::Unavailable)?;
    let mut interval = tokio::time::interval(app.config.delivery_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut failed = false;
    loop {
        tokio::select! { _=stop.changed()=>return Ok(()),_=interval.tick()=>{} }
        let result = async {
            app.repository.cleanup().await?;
            app.repository
                .source_watch(app.config.stale_after.as_secs())
                .await?;
            if !app.config.export_enabled {
                return Ok::<_, Error>(());
            }
            let entries = app.repository.next_delivery().await?;
            if entries.is_empty() {
                return Ok(());
            }
            let mut body = Vec::with_capacity(
                entries
                    .iter()
                    .map(|(_, payload, _)| payload.len() + 1)
                    .sum(),
            );
            for (_, payload, _) in &entries {
                body.extend_from_slice(payload);
                body.push(b'\n');
            }
            // Vector's normalized source acknowledges only after its OpenObserve
            // sink accepts the batch. Failure leaves the database outbox pending.
            let successful = client
                .post(app.config.vector_url.clone())
                .basic_auth(
                    "nds",
                    Some(
                        app.config
                            .vector_password
                            .as_ref()
                            .ok_or(Error::Unavailable)?
                            .expose(),
                    ),
                )
                .header("content-type", "application/x-ndjson")
                .body(body)
                .send()
                .await
                .is_ok_and(|response| response.status().is_success());
            app.repository.delivery_result(&entries, successful).await?;
            if !successful {
                return Err(Error::Unavailable);
            }
            Ok(())
        }
        .await;
        if result.is_err() && !failed {
            tracing::error!(event.name="delivery.failed",module="process",error.type="export_unavailable",outcome="error");
            failed = true;
        } else if result.is_ok() && failed {
            tracing::info!(
                event.name = "delivery.recovered",
                module = "process",
                outcome = "ok"
            );
            failed = false;
        }
    }
}

pub async fn run(config: Arc<Config>, signals: Arc<Signals>) -> Result<(), Error> {
    let app = App::new(config.clone(), signals).await?;
    let listener = TcpListener::bind(config.address)
        .await
        .map_err(|_| Error::Unavailable)?;
    let router = router(app.clone());
    let permits = Arc::new(Semaphore::new(32));
    let mut connections = JoinSet::new();
    let (stop, sleep) = watch::channel(false);
    let mut worker = tokio::spawn(delivery(app.clone(), sleep));
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| Error::Unavailable)?;
    let shutdown = async {
        #[cfg(unix)]
        tokio::select! { _=tokio::signal::ctrl_c()=>{},_=terminate.recv()=>{} }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    };
    tokio::pin!(shutdown);
    tracing::info!(
        event.name = "server.started",
        module = "process",
        outcome = "ok"
    );
    let mut failed = false;
    loop {
        tokio::select! {
            _=&mut shutdown=>break,
            _=&mut worker=>{failed=true;break;},
            Some(_)=connections.join_next(),if !connections.is_empty()=>{},
            accepted=listener.accept()=>{
                let Ok((stream,_))=accepted else{failed=true;break;};
                let Ok(permit)=permits.clone().try_acquire_owned() else{drop(stream);continue;};
                let service=hyper_util::service::TowerToHyperService::new(router.clone());
                connections.spawn(async move{
                    let _permit=permit;
                    // Keep-alive disabled gives every connection a finite total
                    // lifetime, including peers which stop sending bodies.
                    let mut builder=http1::Builder::new();builder.timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(3)).max_headers(32).max_buf_size(32768).keep_alive(false);
                    let _=tokio::time::timeout(Duration::from_secs(12),builder.serve_connection(TokioIo::new(stream),service)).await;
                });
            }
        }
    }
    tracing::info!(
        event.name = "server.shutdown.started",
        module = "process",
        deadline_seconds = 15
    );
    let _ = stop.send(true);
    let shutdown_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let drained = tokio::time::timeout_at(shutdown_deadline, async {
        while connections.join_next().await.is_some() {}
        if !worker.is_finished() {
            let _ = (&mut worker).await;
        }
    })
    .await;
    if drained.is_err() {
        connections.abort_all();
        worker.abort();
    }
    let _ = tokio::time::timeout_at(shutdown_deadline, app.repository.pool.close()).await;
    tracing::info!(
        event.name = "server.shutdown.completed",
        module = "process",
        outcome = if failed { "error" } else { "ok" }
    );
    if failed {
        Err(Error::Unavailable)
    } else {
        Ok(())
    }
}
