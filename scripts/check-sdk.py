#!/usr/bin/env python3
"""Exercise actual concurrent process stdout, without retaining runtime events."""
import json
import pathlib
import subprocess
ROOT=pathlib.Path(__file__).resolve().parents[1]
subprocess.run(['cargo','build','--locked','-p','nddev-device-sync-telemetry','--example','sdk_probe'],cwd=ROOT,check=True)
result=subprocess.run([ROOT/'target/debug/examples/sdk_probe'],capture_output=True,check=True,timeout=5)
assert len(result.stdout)<128*1024
records=[json.loads(line) for line in result.stdout.splitlines()]
assert len(records)==43
assert [event['producer.sequence'] for event in records]==list(range(1,44))
assert len({event['producer.instance_id'] for event in records})==1
assert sum(event['event.name']=='logging.debug.disabled' and event['reason']=='expired' for event in records)==1
assert not any(event['event.name']=='diagnostic.after_expiry' for event in records)
operations=[event for event in records if event['event.name']=='operation.completed']
assert len(operations)==32
assert len({event['trace_id'] for event in operations})==8
assert all(len(event['span_id'])==16 for event in operations)
assert all(len(line)<=16384 for line in result.stdout.splitlines())
assert not result.stderr
print('SDK actual process: 43 complete ordered events, 8 real operation traces, autonomous debug expiry passed')

# This pipe deliberately remains undrained until after the child exits. The
# worker may block in the OS; caller work and Logger::drop must still finish.
blocked=subprocess.Popen([ROOT/'target/debug/examples/sdk_probe','--blocked-stdout'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
try:
    assert blocked.wait(timeout=6)==0
    evidence=json.loads(blocked.stderr.read(4096))
    assert evidence['dropped']>0 and evidence['shutdown_ms']<2500 and evidence['terminated']
    print('SDK undrained stdout: bounded work/shutdown, explicit queue loss, post-shutdown writes rejected')
finally:
    if blocked.poll() is None:blocked.kill();blocked.wait(timeout=2)
    blocked.stdout.close();blocked.stderr.close()
