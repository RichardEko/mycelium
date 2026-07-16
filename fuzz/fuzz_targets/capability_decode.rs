#![no_main]
//! Fuzz target for the capability subsystem's decoders. Feeds the same byte
//! slice through every decoder so a single bad payload hits all five paths:
//! Capability, CapFilter, CapabilityGroupDef, LocalityPath, LoadState.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = mycelium::fuzz_internals::capability_decode(data);
    let _ = mycelium::fuzz_internals::cap_filter_decode(data);
    let _ = mycelium::fuzz_internals::capability_group_def_decode(data);
    let _ = mycelium::fuzz_internals::locality_path_decode(data);
    let _ = mycelium::fuzz_internals::load_state_decode(data);
    // Decode→process: also drive a decoded CapEntry through is_fresh, so the value-processing
    // arithmetic (not just the decoder) is fuzzed — the peer-supplied-arithmetic family.
    let _ = mycelium::fuzz_internals::cap_entry_is_fresh(data);
});
