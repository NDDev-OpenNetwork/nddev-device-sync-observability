use wasm_bindgen::prelude::*;
#[wasm_bindgen(module = "/src/output/console.js")]
extern "C" {
    #[wasm_bindgen(js_name = emitConsole)]
    fn emit_console(line: &str) -> bool;
}
pub(super) fn write(line: &str) -> bool {
    emit_console(line)
}
