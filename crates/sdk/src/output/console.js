// Only the canonical Rust SDK's validated/redacted envelope reaches this sink.
// Never forward a browser exception object, stack, URL or response body here.
export function emitConsole(line) {
  try {
    const severity = JSON.parse(line).severity;
    const method = severity === 'error' ? 'error' : severity === 'warn' ? 'warn' : 'log';
    console[method](line.trimEnd());
    return true;
  } catch {
    return false;
  }
}
