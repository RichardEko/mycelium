#![no_main]
//! Decode→process fuzz target: feed the byte slice through the full wire decoder and, for a Data
//! frame, drive the decoded update end-to-end (hlc.observe → apply_and_notify → hlc.tick, drift
//! disabled). Fuzzes the value-processing surface — the peer-supplied-arithmetic + LWW family — not
//! just the decoder (audit 2026-07-15 sweep).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = mycelium::fuzz_internals::wire_frame_apply(data);
});
