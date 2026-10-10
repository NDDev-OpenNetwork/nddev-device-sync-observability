#!/usr/bin/env python3
"""Run actual SDK/native-sink checks on an explicitly selected owned Android device.

Does not install an app, clear Logcat, change system properties or stop the device.
The application owner separately verifies the production Flutter APK.
"""
import argparse
import json
import os
import pathlib
import shlex
import subprocess
import tempfile
import tomllib
import uuid
from protocol_source import ROOT, source

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--serial', required=True)
parser.add_argument('--ndk', default=os.environ.get('ANDROID_NDK_HOME'))
parser.add_argument('--target', choices=['x86_64-linux-android', 'aarch64-linux-android'], default='x86_64-linux-android')
options = parser.parse_args()
assert options.ndk, '--ndk or ANDROID_NDK_HOME is required'
toolchain = pathlib.Path(options.ndk) / 'toolchains/llvm/prebuilt/linux-x86_64/bin'
linker = toolchain / (options.target + '35-clang')
assert linker.is_file(), 'Android API35 NDK linker is required'
environment = os.environ | {'CARGO_TARGET_' + options.target.upper().replace('-', '_') + '_LINKER': str(linker)}
channel = tomllib.loads((ROOT / 'rust-toolchain.toml').read_text())['toolchain']['channel']

def run(*args, **kwargs):
    return subprocess.run(args, check=True, capture_output=True, text=True, timeout=kwargs.pop('timeout', 30), **kwargs).stdout

def cargo(*args):
    output = run('rustup', 'run', channel, 'cargo', *args, '-p', 'nddev-device-sync-telemetry', '--locked', '--target', options.target, '--message-format=json', cwd=ROOT, env=environment, timeout=300)
    return [json.loads(line) for line in output.splitlines() if line.startswith('{')]

artifacts = cargo('test', '--no-run')
tests = [entry['executable'] for entry in artifacts if entry.get('reason') == 'compiler-artifact' and entry.get('executable') and entry['profile']['test']]
assert len(tests) == 1, 'expected one SDK test executable'
artifacts = cargo('build', '--example', 'sdk_probe')
probes = [entry['executable'] for entry in artifacts if entry.get('reason') == 'compiler-artifact' and entry.get('executable') and entry['target']['name'] == 'sdk_probe']
assert len(probes) == 1
adb = ['adb', '-s', options.serial]
directory = '/data/local/tmp/nds-sdk-' + uuid.uuid4().hex
run(*adb, 'shell', 'mkdir', '-m', '700', directory)
try:
    for local, name in [(tests[0], 'tests'), (probes[0], 'probe')]:
        run(*adb, 'push', local, directory + '/' + name)
        run(*adb, 'shell', 'chmod', '700', directory + '/' + name)
    result = run(*adb, 'shell', directory + '/tests', '--test-threads=1', timeout=30)
    assert 'test result: ok.' in result
    # Reproduce the app's null stdout/stderr; liblog must carry complete events.
    command = shlex.quote(directory + '/probe') + ' >/dev/null 2>&1 & nds_probe_pid=$!; echo "$nds_probe_pid"; wait "$nds_probe_pid"'
    pid = run(*adb, 'shell', command, timeout=10).strip()
    assert pid.isdecimal()
    data = run(*adb, 'logcat', '-d', '-v', 'raw', '--pid=' + pid, '-s', 'NDS:V', '*:S')
    assert len(data.encode()) <= 128 * 1024
    lines = [line for line in data.splitlines() if line.startswith('{')]
    records = [json.loads(line) for line in lines]
    assert len(records) == 43, 'native output missing or fragmented'
    assert [v['producer.sequence'] for v in records] == list(range(1, 44))
    assert len({v['producer.instance_id'] for v in records}) == 1
    assert sum(v['severity'] == 'debug' for v in records) == 8
    assert all(len(line.encode()) <= 3900 for line in lines)
    assert not any(v['event.name'] == 'diagnostic.after_expiry' for v in records)
    assert sum(v['event.name'] == 'logging.debug.disabled' and v['reason'] == 'expired' for v in records) == 1
    operations = [v for v in records if v['event.name'] == 'operation.completed']
    assert len(operations) == 32 and len({v['trace_id'] for v in operations}) == 8
    # Use the same hash-locked validator dependencies as the canonical protocol.
    with tempfile.TemporaryDirectory(prefix='nds-android-schema-') as temporary:
        path = pathlib.Path(temporary)
        (path / 'requirements.lock').write_bytes(source('requirements.lock'))
        schema = source('contracts/v2/telemetry-event.schema.json')
        assert schema == (ROOT / 'crates/sdk/telemetry-event.schema.json').read_bytes()
        (path / 'schema.json').write_bytes(schema)
        run('python3', '-m', 'venv', str(path / 'venv'), timeout=30)
        python = str(path / 'venv/bin/python')
        run(python, '-m', 'pip', '--isolated', 'install', '--require-hashes', '-r', str(path / 'requirements.lock'), timeout=120)
        verify = 'import json,sys;from jsonschema import Draft202012Validator,FormatChecker;check=Draft202012Validator(json.load(open(sys.argv[1])),format_checker=FormatChecker());assert all(check.is_valid(json.loads(line)) for line in sys.stdin if line.strip()),"canonical event constraint rejected"'
        run(python, '-c', verify, str(path / 'schema.json'), input='\n'.join(lines) + '\n')
finally:
    run(*adb, 'shell', 'rm', '-rf', directory)
print('Android native SDK: actual liblog, 43 complete canonical events with null stdout, bounded debug expiry, adapter oversize/invalid-record loss accounting passed')
