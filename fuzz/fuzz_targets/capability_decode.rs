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
});
