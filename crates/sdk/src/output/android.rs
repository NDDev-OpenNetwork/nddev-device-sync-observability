use android_log_sys::{__android_log_write, LogPriority};
use std::{
    ffi::CString,
    io::{self, Write},
};

// Below liblog's 4068-byte payload bound, including priority, tag and NULs.
// Never split or truncate a JSON event into unparseable Logcat fragments.
const MAX_JSON_BYTES: usize = 3900;
pub(super) struct Logcat;
impl Write for Logcat {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let message = bytes.strip_suffix(b"\n").unwrap_or(bytes);
        if message.len() > MAX_JSON_BYTES {
            return Err(io::Error::other("native telemetry byte budget"));
        }
        let event: serde_json::Value = serde_json::from_slice(message)
            .map_err(|_| io::Error::other("invalid native telemetry event"))?;
        let priority = match event.get("severity").and_then(serde_json::Value::as_str) {
            Some("warn") => LogPriority::WARN,
            Some("error") => LogPriority::ERROR,
            // SDK policy has already admitted bounded diagnostics. INFO is the
            // transport floor; JSON keeps the canonical severity. Changing the
            // process-global Android filter would also expose dependency logs.
            Some("info" | "debug" | "trace") => LogPriority::INFO,
            _ => return Err(io::Error::other("invalid native telemetry severity")),
        };
        let message = CString::new(message)
            .map_err(|_| io::Error::other("invalid native telemetry string"))?;
        // SAFETY: both pointers reference owned, NUL-terminated strings alive
        // for this synchronous call; no printf interpretation or retained data.
        let accepted =
            unsafe { __android_log_write(priority as i32, c"NDS".as_ptr(), message.as_ptr()) };
        if accepted <= 0 {
            return Err(io::Error::other("native telemetry write rejected"));
        }
        // liblog acceptance is not proof of durable storage or later retrieval.
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actual_native_sink_counts_oversize_and_invalid_records_without_truncation() {
        let (mut writer, mut worker) = crate::output::start(crate::LocalOutput::Stdout).unwrap();
        writer
            .write_all(b"{\"severity\":\"info\",\"event.name\":\"sdk.native.accepted\"}\n")
            .unwrap();
        writer.write_all(&vec![b'x'; MAX_JSON_BYTES + 1]).unwrap();
        writer.write_all(b"{\"severity\":\"invalid\"}\n").unwrap();
        worker.close();
        let state = writer.state.snapshot();
        assert_eq!(state.pending_events, 0);
        assert_eq!(state.write_failures, 2);
        assert_eq!(state.dropped_events, 2);
        assert!(state.terminating);
        assert!(!state.shutdown_incomplete);
    }
}
