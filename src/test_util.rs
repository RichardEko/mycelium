//! Shared, test-only helpers (crate-internal).

use std::net::TcpListener;
use std::sync::atomic::{AtomicU32, Ordering};

/// Allocate a loopback port that **no other test in this process will target**.
///
/// The per-module helpers this replaces bound `127.0.0.1:0`, took the OS-assigned port, and dropped
/// the socket — but the OS readily hands the *same* just-freed port to a concurrent caller, so two
/// parallel `#[tokio::test]`s raced to bind it and one died with `AddrInUse` (errno 48/98). That was
/// the recurring CI flake family (`scatter_gather_two_of_two`, `test_gateway_port_closes_on_shutdown`,
/// the forwarding relay, …).
///
/// This hands out **process-unique** candidates from a shared atomic counter — seeded per-process
/// (by pid) so parallel `cargo test` binaries start in different windows — and bind-verifies each.
/// Because every test gets a distinct number, no sibling test is chasing the same number when the
/// agent finally binds it.
///
/// **The candidate range is kept strictly below the OS ephemeral floor** (Linux
/// `ip_local_port_range` default 32768, macOS/Windows 49152). This closes the last residual race
/// (analysis Run 27, finding: `test_wsc_m8_auto_config_cluster_converges` flaking ~1/3 under
/// parallel load): an agent under test opens *many* outbound gossip/RPC connections, and the OS
/// assigns those an ephemeral *source* port — which, with the old 20000..60000 range, could land on
/// a port `alloc_port` had returned but whose agent had not yet bound, so `start()` died with
/// `AddrInUse`. Confining candidates to 16000..32000 (all `< 32768`) means the OS never
/// auto-assigns a colliding port; the only remaining loss is an unrelated process holding a port,
/// which bind-verify skips.
pub(crate) fn alloc_port() -> u16 {
    const LO: u32 = 16_000;
    const SPAN: u32 = 16_000; // 16000..32000 — wholly below the OS ephemeral floor (32768)
    static NEXT: AtomicU32 = AtomicU32::new(0);
    // Seed once per process into a pid-derived window so parallel test binaries don't overlap.
    let seed = (std::process::id().wrapping_mul(2_654_435_761) % SPAN) + 1; // never 0 (the sentinel)
    let _ = NEXT.compare_exchange(0, seed, Ordering::Relaxed, Ordering::Relaxed);

    for _ in 0..SPAN {
        let p = (LO + NEXT.fetch_add(1, Ordering::Relaxed) % SPAN) as u16;
        if TcpListener::bind(("127.0.0.1", p)).is_ok() {
            return p;
        }
    }
    panic!("no free loopback port found");
}
