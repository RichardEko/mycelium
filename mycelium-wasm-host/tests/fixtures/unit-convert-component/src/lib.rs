//! A kg→tonnes unit-converter capability component — the code that *arrives* in the
//! `mcp_toolgrowth` demo. The conversion arithmetic lives here, inside the sandboxed guest:
//! no node has it compiled in; it is pulled by content address, verified, and instantiated at
//! runtime. Contract: request payload `{"kg": <number>}` → response payload
//! `{"tonnes": <number>}`; a malformed payload is a component-level error, not a crash.

wit_bindgen::generate!({
    world: "capability-component",
    path: "../../../wit",
});

use exports::mycelium::host::capability::{Guest, Request, Response};

struct Component;

impl Guest for Component {
    fn handle(req: Request) -> Response {
        let v: serde_json::Value = match serde_json::from_slice(&req.payload) {
            Ok(v) => v,
            Err(e) => {
                return Response { payload: Vec::new(), error: Some(format!("bad json: {e}")) }
            }
        };
        let Some(kg) = v.get("kg").and_then(|k| k.as_f64()) else {
            return Response { payload: Vec::new(), error: Some("missing \"kg\" field".into()) };
        };
        mycelium::host::log::info(&format!("unit-convert: {kg} kg"));
        let out = serde_json::json!({ "tonnes": kg / 1000.0 });
        Response { payload: out.to_string().into_bytes(), error: None }
    }
}

export!(Component);
