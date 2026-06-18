//! Minimal capability component for the host⇄component e2e test. It exercises the host boundary:
//! on `handle`, it stores the request payload under a component-relative key via the `kv` import,
//! reads it back, logs via the `log` import, and echoes it as the response. The host test then
//! asserts the write landed under the confined `comp/{node}/{ns}/…` subtree.

wit_bindgen::generate!({
    world: "capability-component",
    path: "../../../wit",
});

use exports::mycelium::host::capability::{Guest, Request, Response};

struct Component;

impl Guest for Component {
    fn handle(req: Request) -> Response {
        // Round-trip through the host's scoped KV import (proves kv.set/get cross the boundary).
        mycelium::host::kv::set("last-input", &req.payload);
        let echoed = mycelium::host::kv::get("last-input").unwrap_or_default();
        mycelium::host::log::info(&format!("echo handled kind={}", req.kind));
        Response { payload: echoed, error: None }
    }
}

export!(Component);
