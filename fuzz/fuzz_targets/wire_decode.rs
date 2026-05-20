#![no_main]
//! Fuzz target for `WireMessage` decode. Feeds arbitrary bytes through the
//! same bincode configuration the live connection handler uses; succeeds if
//! no decode-time panic fires (any `Err` is valid output — fuzzing only
//! catches *unsafe* failures like overflow / OOB / panics).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = mycelium::fuzz_internals::wire_message_decode(data);
});
