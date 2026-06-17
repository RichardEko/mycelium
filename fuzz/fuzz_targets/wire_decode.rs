#![no_main]
//! Fuzz target for `WireMessage` decode. Feeds arbitrary bytes through the
//! same in-tree `codec::decode_wire` the live connection handler uses (M11,
//! replacing bincode); succeeds if no decode-time panic fires (any `Err` is
//! valid output — fuzzing only catches *unsafe* failures like overflow / OOB
//! / panics).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = mycelium::fuzz_internals::wire_message_decode(data);
});
