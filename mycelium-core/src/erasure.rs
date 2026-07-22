//! Crypto-shredding for GDPR right-to-erasure (SOC 2 WS-F).
//!
//! Physical deletion is not guaranteeable in a gossip+WAL mesh (tombstone windows, anti-entropy
//! resurrection, WAL/snapshots/replicas — see `docs/design/data-lifecycle-and-erasure.md`). The
//! recognised answer for such stores is **crypto-shredding**: encrypt each data subject's personal
//! data under a **per-subject key (DEK)**; "erase subject S" = **destroy S's DEK**. Every ciphertext
//! copy — live KV, WAL, snapshot, replica, old backup — becomes cryptographically unrecoverable the
//! instant the DEK is gone, without touching a single distributed byte. Erasure is an O(1)
//! key-destroy, not an unbounded byte-hunt.
//!
//! This [`SubjectKeyRegistry`] is a **reference helper** (AES-256-GCM via ring). The app
//! envelope-encrypts PII with [`encrypt_for`](SubjectKeyRegistry::encrypt_for) before `kv.set`,
//! [`decrypt_for`](SubjectKeyRegistry::decrypt_for)s on read, and calls
//! [`destroy`](SubjectKeyRegistry::destroy) to erase.
//!
//! **Production custody (the honest limit):** the reference holds DEKs in memory. For a durable,
//! provably-destroyable store, back each DEK with a **KMS** (generate/wrap the DEK in the KMS;
//! `destroy` = KMS delete-key), so destruction survives restarts and is enforced against DEK
//! backups. Composes with [`crate::persistence::DataAtRestCipher`] (disk-boundary encryption): this
//! is the per-subject layer *above* the KV value; the two are defence-in-depth.
//!
//! **Other limits (must be documented to the operator):** PII placed in the audit trail (`detail`),
//! logs, or metric labels is **not** covered by DEK destruction — keep subject PII in
//! envelope-encrypted KV values only. A backup that captured both ciphertext *and* the DEK before
//! destruction can restore the data — custody must guarantee DEK destruction reaches DEK backups
//! (KMS enforces this).
//!
//! Gated behind `tls` (reuses ring, already in-tree via rustls — no new compiled crate).

#![cfg(feature = "tls")]

use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};
use ring::rand::{SecureRandom, SystemRandom};
use std::collections::HashMap;
use std::sync::Mutex;

/// A per-subject data-encryption-key registry for crypto-shredding.
///
/// Keys are identified by an opaque subject id (`String` — e.g. a user/customer id). Cloneable
/// handles share the same registry via an internal `Arc`-free `Mutex` (wrap the registry in an
/// `Arc` to share across tasks). Not itself persistent — see the module doc on KMS-backed custody
/// for a durable, provably-erasable deployment.
#[derive(Default)]
pub struct SubjectKeyRegistry {
    keys: Mutex<HashMap<String, [u8; 32]>>,
}

impl SubjectKeyRegistry {
    /// A fresh, empty registry.
    pub fn new() -> Self {
        Self { keys: Mutex::new(HashMap::new()) }
    }

    /// True if `subject` currently has a live DEK (i.e. has data that is *not* erased).
    pub fn contains(&self, subject: &str) -> bool {
        self.lock().contains_key(subject)
    }

    /// Number of subjects with live keys.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// True if no subject has a live key.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Envelope-encrypt `plaintext` under `subject`'s DEK, creating the DEK on first use. The
    /// returned blob is `nonce(12) ‖ ciphertext ‖ tag(16)` — store it wherever you'd have stored
    /// the plaintext (a KV value). Encrypting after a [`destroy`](Self::destroy) mints a **new**
    /// DEK (new data under a new key); previously-encrypted ciphertext stays unrecoverable.
    pub fn encrypt_for(&self, subject: &str, plaintext: &[u8]) -> Vec<u8> {
        let dek = self.get_or_create(subject);
        let key = LessSafeKey::new(
            UnboundKey::new(&AES_256_GCM, &dek).expect("AES-256 key is 32 bytes"),
        );
        let mut nonce_bytes = [0u8; NONCE_LEN];
        SystemRandom::new()
            .fill(&mut nonce_bytes)
            .expect("system RNG");
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);
        let mut in_out = plaintext.to_vec();
        key.seal_in_place_append_tag(nonce, Aad::empty(), &mut in_out)
            .expect("seal never fails for a valid key/nonce");
        let mut out = Vec::with_capacity(NONCE_LEN + in_out.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&in_out);
        out
    }

    /// Decrypt a blob produced by [`encrypt_for`](Self::encrypt_for). Returns `None` if the subject
    /// has been **erased** (no DEK — the whole point), if the blob is malformed, or if
    /// authentication fails.
    pub fn decrypt_for(&self, subject: &str, blob: &[u8]) -> Option<Vec<u8>> {
        if blob.len() < NONCE_LEN + 16 {
            return None;
        }
        let dek = *self.lock().get(subject)?;
        let key = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &dek).ok()?);
        let (nonce_bytes, ct) = blob.split_at(NONCE_LEN);
        let nonce = Nonce::try_assume_unique_for_key(nonce_bytes).ok()?;
        let mut in_out = ct.to_vec();
        let plain = key.open_in_place(nonce, Aad::empty(), &mut in_out).ok()?;
        Some(plain.to_vec())
    }

    /// **Erase** `subject`: destroy its DEK. All ciphertext ever produced for it becomes
    /// permanently undecryptable — the GDPR-erasure primitive. Returns `true` if a key was
    /// present. The DEK bytes are zeroized before being dropped. (Best-effort in-process zeroize;
    /// production KMS custody makes destruction durable and backup-safe — see the module doc.)
    pub fn destroy(&self, subject: &str) -> bool {
        match self.lock().remove(subject) {
            Some(mut dek) => {
                // Overwrite the key material before it drops. `write_volatile` is not optimised
                // away; sufficient for the reference (a KMS is the production custody boundary).
                for b in dek.iter_mut() {
                    unsafe { std::ptr::write_volatile(b, 0u8) };
                }
                true
            }
            None => false,
        }
    }

    /// Insert a DEK for `subject` from external custody (e.g. unwrapped from a KMS) — the seam a
    /// KMS-backed deployment uses instead of the in-memory `get_or_create`. Overwrites any existing
    /// key for the subject.
    pub fn install_key(&self, subject: impl Into<String>, dek: [u8; 32]) {
        self.lock().insert(subject.into(), dek);
    }

    fn get_or_create(&self, subject: &str) -> [u8; 32] {
        let mut guard = self.lock();
        if let Some(k) = guard.get(subject) {
            return *k;
        }
        let mut dek = [0u8; 32];
        SystemRandom::new().fill(&mut dek).expect("system RNG");
        guard.insert(subject.to_string(), dek);
        dek
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, [u8; 32]>> {
        self.keys.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_isolates_subjects() {
        let reg = SubjectKeyRegistry::new();
        let ct_a = reg.encrypt_for("alice", b"alice PII");
        let ct_b = reg.encrypt_for("bob", b"bob PII");
        assert_eq!(reg.decrypt_for("alice", &ct_a).as_deref(), Some(&b"alice PII"[..]));
        assert_eq!(reg.decrypt_for("bob", &ct_b).as_deref(), Some(&b"bob PII"[..]));
        // A subject cannot decrypt another's ciphertext.
        assert_eq!(reg.decrypt_for("bob", &ct_a), None);
    }

    #[test]
    fn destroy_makes_ciphertext_unrecoverable() {
        let reg = SubjectKeyRegistry::new();
        let ct = reg.encrypt_for("carol", b"erase me");
        assert!(reg.decrypt_for("carol", &ct).is_some());

        // Erasure: destroy the DEK. The ciphertext bytes still exist (they'd linger in KV/WAL/
        // backups) but are now cryptographically dead.
        assert!(reg.destroy("carol"));
        assert!(!reg.contains("carol"));
        assert_eq!(reg.decrypt_for("carol", &ct), None, "erased subject is undecryptable");

        // Re-encrypting mints a new DEK; the OLD ciphertext stays dead under the new key.
        let ct2 = reg.encrypt_for("carol", b"new data");
        assert_eq!(reg.decrypt_for("carol", &ct2).as_deref(), Some(&b"new data"[..]));
        assert_eq!(reg.decrypt_for("carol", &ct), None, "old ciphertext never revives");

        // Destroying a non-existent subject is a no-op false.
        assert!(!reg.destroy("nobody"));
    }

    #[test]
    fn tampered_ciphertext_fails_authentication() {
        let reg = SubjectKeyRegistry::new();
        let mut ct = reg.encrypt_for("dave", b"authentic");
        let last = ct.len() - 1;
        ct[last] ^= 0xff; // flip a tag bit
        assert_eq!(reg.decrypt_for("dave", &ct), None, "AEAD rejects tampering");
    }

    #[test]
    fn install_key_supports_external_custody() {
        let reg = SubjectKeyRegistry::new();
        // Simulate a KMS-unwrapped DEK shared between two registry instances (e.g. two nodes).
        let dek = [42u8; 32];
        reg.install_key("erin", dek);
        let ct = reg.encrypt_for("erin", b"kms-backed");
        let reg2 = SubjectKeyRegistry::new();
        reg2.install_key("erin", dek);
        assert_eq!(reg2.decrypt_for("erin", &ct).as_deref(), Some(&b"kms-backed"[..]));
    }
}
