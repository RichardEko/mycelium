//! A self-contained serde binary format, byte-identical to
//! `bincode 2.x` `standard().with_fixed_int_encoding()` (WS-B M11).
//!
//! This is the maintained, in-tree replacement for the unmaintained `bincode`
//! crate (RUSTSEC-2025-0141). Every `#[derive(Serialize, Deserialize)]` type that
//! previously round-tripped through `bincode::serde::{encode,decode}` now flows
//! through [`to_vec`] / [`from_slice`], producing the *same bytes* — so this is a
//! drop-in swap with no on-disk migration and no signature/hash-chain breakage
//! (audit chains, consensus signatures, and KV-stored capability bytes are all
//! computed over these bytes). The `tests` module proves equivalence against
//! `bincode` for representative types; `bincode` is retained only as a
//! `dev-dependency` oracle.
//!
//! ## Layout (the bincode fixed-int subset we reproduce)
//! - integers: little-endian, fixed width (`i8..=i128`, `u8..=u128`);
//! - `bool`: one byte (`0`/`1`); `char`: `u32` LE scalar value;
//! - `f32`/`f64`: little-endian IEEE-754 bits;
//! - enum variant tag: `u32` LE discriminant (declaration order);
//! - `Option<T>`: one byte tag (`0`=None, `1`=Some) then `T`;
//! - sequence / map / `str` / byte-slice length: `u64` LE, then the elements;
//! - fixed-size array / tuple / struct: elements concatenated, no length prefix.
//!
//! Decoding is **not self-describing** (exactly like bincode): the target type
//! drives the reads. `deserialize_any` is unsupported and errors, so a malformed
//! frame can never coerce an unexpected shape.

use serde::{de, ser};
use std::fmt::{self, Display};

/// Upper bound on any single decode (mirrors the old `bincode_cfg().with_limit`).
/// A length prefix is additionally capped to the bytes actually remaining, so a
/// hostile count can never drive an unbounded `Vec::with_capacity`.
pub const MAX_DECODE_BYTES: usize = 10 * 1024 * 1024;

/// Encode/decode error. Carries an owned message for diagnostics; callers that
/// previously dropped a frame on a `bincode` error drop it the same way here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(pub String);

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fixint codec error: {}", self.0)
    }
}
impl std::error::Error for Error {}
impl ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self { Error(msg.to_string()) }
}
impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self { Error(msg.to_string()) }
}

// ── Serializer ────────────────────────────────────────────────────────────────

/// Serialize `value` into a fresh `Vec<u8>`, byte-identical to bincode fixed-int.
pub fn to_vec<T: ser::Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, Error> {
    let mut out = Vec::with_capacity(64);
    value.serialize(Serializer { out: &mut out })?;
    Ok(out)
}

struct Serializer<'a> {
    out: &'a mut Vec<u8>,
}

impl Serializer<'_> {
    #[inline]
    fn put_len(&mut self, n: usize) { self.out.extend_from_slice(&(n as u64).to_le_bytes()); }
    #[inline]
    fn put_variant(&mut self, idx: u32) { self.out.extend_from_slice(&idx.to_le_bytes()); }
}

impl<'a> ser::Serializer for Serializer<'a> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Compound<'a>;
    type SerializeTuple = Compound<'a>;
    type SerializeTupleStruct = Compound<'a>;
    type SerializeTupleVariant = Compound<'a>;
    type SerializeMap = Compound<'a>;
    type SerializeStruct = Compound<'a>;
    type SerializeStructVariant = Compound<'a>;

    fn serialize_bool(self, v: bool) -> Result<(), Error> { self.out.push(v as u8); Ok(()) }
    fn serialize_i8(self, v: i8) -> Result<(), Error> { self.out.push(v as u8); Ok(()) }
    fn serialize_i16(self, v: i16) -> Result<(), Error> { self.out.extend_from_slice(&v.to_le_bytes()); Ok(()) }
    fn serialize_i32(self, v: i32) -> Result<(), Error> { self.out.extend_from_slice(&v.to_le_bytes()); Ok(()) }
    fn serialize_i64(self, v: i64) -> Result<(), Error> { self.out.extend_from_slice(&v.to_le_bytes()); Ok(()) }
    fn serialize_i128(self, v: i128) -> Result<(), Error> { self.out.extend_from_slice(&v.to_le_bytes()); Ok(()) }
    fn serialize_u8(self, v: u8) -> Result<(), Error> { self.out.push(v); Ok(()) }
    fn serialize_u16(self, v: u16) -> Result<(), Error> { self.out.extend_from_slice(&v.to_le_bytes()); Ok(()) }
    fn serialize_u32(self, v: u32) -> Result<(), Error> { self.out.extend_from_slice(&v.to_le_bytes()); Ok(()) }
    fn serialize_u64(self, v: u64) -> Result<(), Error> { self.out.extend_from_slice(&v.to_le_bytes()); Ok(()) }
    fn serialize_u128(self, v: u128) -> Result<(), Error> { self.out.extend_from_slice(&v.to_le_bytes()); Ok(()) }
    fn serialize_f32(self, v: f32) -> Result<(), Error> { self.out.extend_from_slice(&v.to_le_bytes()); Ok(()) }
    fn serialize_f64(self, v: f64) -> Result<(), Error> { self.out.extend_from_slice(&v.to_le_bytes()); Ok(()) }
    fn serialize_char(self, v: char) -> Result<(), Error> { self.serialize_u32(v as u32) }

    fn serialize_str(mut self, v: &str) -> Result<(), Error> {
        self.put_len(v.len());
        self.out.extend_from_slice(v.as_bytes());
        Ok(())
    }
    fn serialize_bytes(mut self, v: &[u8]) -> Result<(), Error> {
        self.put_len(v.len());
        self.out.extend_from_slice(v);
        Ok(())
    }

    fn serialize_none(self) -> Result<(), Error> { self.out.push(0); Ok(()) }
    fn serialize_some<T: ser::Serialize + ?Sized>(self, value: &T) -> Result<(), Error> {
        self.out.push(1);
        value.serialize(Serializer { out: self.out })
    }

    fn serialize_unit(self) -> Result<(), Error> { Ok(()) }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), Error> { Ok(()) }
    fn serialize_unit_variant(mut self, _name: &'static str, idx: u32, _variant: &'static str) -> Result<(), Error> {
        self.put_variant(idx);
        Ok(())
    }
    fn serialize_newtype_struct<T: ser::Serialize + ?Sized>(self, _name: &'static str, value: &T) -> Result<(), Error> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: ser::Serialize + ?Sized>(
        mut self, _name: &'static str, idx: u32, _variant: &'static str, value: &T,
    ) -> Result<(), Error> {
        self.put_variant(idx);
        value.serialize(Serializer { out: self.out })
    }

    fn serialize_seq(mut self, len: Option<usize>) -> Result<Compound<'a>, Error> {
        let len = len.ok_or_else(|| Error("sequence length must be known".into()))?;
        self.put_len(len);
        Ok(Compound { out: self.out })
    }
    fn serialize_tuple(self, _len: usize) -> Result<Compound<'a>, Error> { Ok(Compound { out: self.out }) }
    fn serialize_tuple_struct(self, _name: &'static str, _len: usize) -> Result<Compound<'a>, Error> {
        Ok(Compound { out: self.out })
    }
    fn serialize_tuple_variant(
        mut self, _name: &'static str, idx: u32, _variant: &'static str, _len: usize,
    ) -> Result<Compound<'a>, Error> {
        self.put_variant(idx);
        Ok(Compound { out: self.out })
    }
    fn serialize_map(mut self, len: Option<usize>) -> Result<Compound<'a>, Error> {
        let len = len.ok_or_else(|| Error("map length must be known".into()))?;
        self.put_len(len);
        Ok(Compound { out: self.out })
    }
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Compound<'a>, Error> {
        Ok(Compound { out: self.out })
    }
    fn serialize_struct_variant(
        mut self, _name: &'static str, idx: u32, _variant: &'static str, _len: usize,
    ) -> Result<Compound<'a>, Error> {
        self.put_variant(idx);
        Ok(Compound { out: self.out })
    }

    fn is_human_readable(&self) -> bool { false }
}

/// Shared state for every compound (seq/tuple/struct/map/variant) serializer.
/// Fields are written inline with no per-element framing, exactly like bincode.
struct Compound<'a> {
    out: &'a mut Vec<u8>,
}

impl Compound<'_> {
    #[inline]
    fn elem<T: ser::Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        value.serialize(Serializer { out: self.out })
    }
}

impl ser::SerializeSeq for Compound<'_> {
    type Ok = (); type Error = Error;
    fn serialize_element<T: ser::Serialize + ?Sized>(&mut self, v: &T) -> Result<(), Error> { self.elem(v) }
    fn end(self) -> Result<(), Error> { Ok(()) }
}
impl ser::SerializeTuple for Compound<'_> {
    type Ok = (); type Error = Error;
    fn serialize_element<T: ser::Serialize + ?Sized>(&mut self, v: &T) -> Result<(), Error> { self.elem(v) }
    fn end(self) -> Result<(), Error> { Ok(()) }
}
impl ser::SerializeTupleStruct for Compound<'_> {
    type Ok = (); type Error = Error;
    fn serialize_field<T: ser::Serialize + ?Sized>(&mut self, v: &T) -> Result<(), Error> { self.elem(v) }
    fn end(self) -> Result<(), Error> { Ok(()) }
}
impl ser::SerializeTupleVariant for Compound<'_> {
    type Ok = (); type Error = Error;
    fn serialize_field<T: ser::Serialize + ?Sized>(&mut self, v: &T) -> Result<(), Error> { self.elem(v) }
    fn end(self) -> Result<(), Error> { Ok(()) }
}
impl ser::SerializeMap for Compound<'_> {
    type Ok = (); type Error = Error;
    fn serialize_key<T: ser::Serialize + ?Sized>(&mut self, k: &T) -> Result<(), Error> { self.elem(k) }
    fn serialize_value<T: ser::Serialize + ?Sized>(&mut self, v: &T) -> Result<(), Error> { self.elem(v) }
    fn end(self) -> Result<(), Error> { Ok(()) }
}
impl ser::SerializeStruct for Compound<'_> {
    type Ok = (); type Error = Error;
    fn serialize_field<T: ser::Serialize + ?Sized>(&mut self, _k: &'static str, v: &T) -> Result<(), Error> { self.elem(v) }
    fn end(self) -> Result<(), Error> { Ok(()) }
}
impl ser::SerializeStructVariant for Compound<'_> {
    type Ok = (); type Error = Error;
    fn serialize_field<T: ser::Serialize + ?Sized>(&mut self, _k: &'static str, v: &T) -> Result<(), Error> { self.elem(v) }
    fn end(self) -> Result<(), Error> { Ok(()) }
}

// ── Deserializer ───────────────────────────────────────────────────────────────

/// Decode a `T` from the front of `bytes`, **tolerating trailing bytes** — exactly
/// like the `bincode::serde::decode_from_slice(..).map(|(v, _)| v)` it replaces, which
/// every former call site relied on. This is load-bearing: a `cap/` KV value is a
/// `CapEntry` (a `Capability` followed by a trailing `u64` refresh interval), and the
/// A2A card / legacy readers decode just the `Capability` prefix and ignore the suffix
/// (the documented dual-format fallback). The untrusted *wire* path uses the separate,
/// strict [`crate::codec::decode_wire`] (which rejects trailing), so trusted KV / signed
/// payloads decoded here keep bincode's lenient prefix semantics.
pub fn from_slice<'de, T: de::Deserialize<'de>>(bytes: &'de [u8]) -> Result<T, Error> {
    let mut d = Deserializer { buf: bytes, pos: 0 };
    T::deserialize(&mut d)
}

struct Deserializer<'de> {
    buf: &'de [u8],
    pos: usize,
}

impl<'de> Deserializer<'de> {
    #[inline]
    fn remaining(&self) -> usize { self.buf.len() - self.pos }
    #[inline]
    fn take(&mut self, n: usize) -> Result<&'de [u8], Error> {
        if self.remaining() < n {
            return Err(Error("unexpected end of input".into()));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    #[inline]
    fn u8(&mut self) -> Result<u8, Error> { Ok(self.take(1)?[0]) }
    #[inline]
    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    #[inline]
    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    /// A `u64` length prefix, validated to fit the remaining buffer (no unbounded alloc).
    fn read_len(&mut self) -> Result<usize, Error> {
        let n = self.u64()? as usize;
        if n > self.remaining() || n > MAX_DECODE_BYTES {
            return Err(Error("length prefix exceeds input".into()));
        }
        Ok(n)
    }
    /// Capacity hint bounded by the bytes left (each element is ≥ 1 byte).
    fn cap_hint(&self, len: usize) -> usize { len.min(self.remaining()) }
}

macro_rules! de_int {
    ($method:ident, $visit:ident, $ty:ty) => {
        fn $method<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
            const N: usize = std::mem::size_of::<$ty>();
            let b = self.take(N)?;
            visitor.$visit(<$ty>::from_le_bytes(b.try_into().unwrap()))
        }
    };
}

impl<'de> de::Deserializer<'de> for &mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V: de::Visitor<'de>>(self, _v: V) -> Result<V::Value, Error> {
        Err(Error("self-describing decode is unsupported (fixint is not self-describing)".into()))
    }

    fn deserialize_bool<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match self.u8()? {
            0 => visitor.visit_bool(false),
            1 => visitor.visit_bool(true),
            _ => Err(Error("invalid bool".into())),
        }
    }

    de_int!(deserialize_i16, visit_i16, i16);
    de_int!(deserialize_i32, visit_i32, i32);
    de_int!(deserialize_i64, visit_i64, i64);
    de_int!(deserialize_i128, visit_i128, i128);
    de_int!(deserialize_u16, visit_u16, u16);
    de_int!(deserialize_u32, visit_u32, u32);
    de_int!(deserialize_u64, visit_u64, u64);
    de_int!(deserialize_u128, visit_u128, u128);

    fn deserialize_i8<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_i8(self.u8()? as i8)
    }
    fn deserialize_u8<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_u8(self.u8()?)
    }
    fn deserialize_f32<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let b = self.take(4)?;
        visitor.visit_f32(f32::from_le_bytes(b.try_into().unwrap()))
    }
    fn deserialize_f64<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let b = self.take(8)?;
        visitor.visit_f64(f64::from_le_bytes(b.try_into().unwrap()))
    }
    fn deserialize_char<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let c = char::from_u32(self.u32()?).ok_or_else(|| Error("invalid char".into()))?;
        visitor.visit_char(c)
    }

    fn deserialize_str<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let n = self.read_len()?;
        let s = std::str::from_utf8(self.take(n)?).map_err(|_| Error("invalid utf-8".into()))?;
        visitor.visit_borrowed_str(s)
    }
    fn deserialize_string<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        self.deserialize_str(visitor)
    }
    fn deserialize_bytes<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let n = self.read_len()?;
        visitor.visit_borrowed_bytes(self.take(n)?)
    }
    fn deserialize_byte_buf<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match self.u8()? {
            0 => visitor.visit_none(),
            1 => visitor.visit_some(self),
            _ => Err(Error("invalid option tag".into())),
        }
    }
    fn deserialize_unit<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_unit()
    }
    fn deserialize_unit_struct<V: de::Visitor<'de>>(self, _name: &'static str, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_unit()
    }
    fn deserialize_newtype_struct<V: de::Visitor<'de>>(self, _name: &'static str, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let len = self.read_len()?;
        let cap = self.cap_hint(len);
        visitor.visit_seq(Seq { de: self, remaining: len, size_hint: cap })
    }
    fn deserialize_tuple<V: de::Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_seq(Seq { de: self, remaining: len, size_hint: len })
    }
    fn deserialize_tuple_struct<V: de::Visitor<'de>>(self, _name: &'static str, len: usize, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_seq(Seq { de: self, remaining: len, size_hint: len })
    }
    fn deserialize_map<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let len = self.read_len()?;
        let cap = self.cap_hint(len);
        visitor.visit_map(Seq { de: self, remaining: len, size_hint: cap })
    }
    fn deserialize_struct<V: de::Visitor<'de>>(
        self, _name: &'static str, fields: &'static [&'static str], visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_seq(Seq { de: self, remaining: fields.len(), size_hint: fields.len() })
    }
    fn deserialize_enum<V: de::Visitor<'de>>(
        self, _name: &'static str, _variants: &'static [&'static str], visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_enum(Enum { de: self })
    }
    fn deserialize_identifier<V: de::Visitor<'de>>(self, _v: V) -> Result<V::Value, Error> {
        Err(Error("identifier decode is unsupported".into()))
    }
    fn deserialize_ignored_any<V: de::Visitor<'de>>(self, _v: V) -> Result<V::Value, Error> {
        Err(Error("ignored_any is unsupported (not self-describing)".into()))
    }

    fn is_human_readable(&self) -> bool { false }
}

/// Sequence / tuple / struct / map accessor: yields exactly `remaining` elements.
struct Seq<'a, 'de> {
    de: &'a mut Deserializer<'de>,
    remaining: usize,
    size_hint: usize,
}

impl<'de> de::SeqAccess<'de> for Seq<'_, 'de> {
    type Error = Error;
    fn next_element_seed<T: de::DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>, Error> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(&mut *self.de).map(Some)
    }
    fn size_hint(&self) -> Option<usize> { Some(self.size_hint.min(self.remaining)) }
}

impl<'de> de::MapAccess<'de> for Seq<'_, 'de> {
    type Error = Error;
    fn next_key_seed<K: de::DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>, Error> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(&mut *self.de).map(Some)
    }
    fn next_value_seed<V: de::DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, Error> {
        seed.deserialize(&mut *self.de)
    }
    fn size_hint(&self) -> Option<usize> { Some(self.size_hint.min(self.remaining)) }
}

/// Enum accessor: the variant index is a `u32` LE discriminant (bincode layout).
struct Enum<'a, 'de> {
    de: &'a mut Deserializer<'de>,
}

impl<'de> de::EnumAccess<'de> for Enum<'_, 'de> {
    type Error = Error;
    type Variant = Self;
    fn variant_seed<V: de::DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self), Error> {
        let idx = self.de.u32()?;
        let val = seed.deserialize(de::value::U32Deserializer::<Error>::new(idx))?;
        Ok((val, self))
    }
}

impl<'de> de::VariantAccess<'de> for Enum<'_, 'de> {
    type Error = Error;
    fn unit_variant(self) -> Result<(), Error> { Ok(()) }
    fn newtype_variant_seed<T: de::DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, Error> {
        seed.deserialize(&mut *self.de)
    }
    fn tuple_variant<V: de::Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_seq(Seq { de: self.de, remaining: len, size_hint: len })
    }
    fn struct_variant<V: de::Visitor<'de>>(self, fields: &'static [&'static str], visitor: V) -> Result<V::Value, Error> {
        visitor.visit_seq(Seq { de: self.de, remaining: fields.len(), size_hint: fields.len() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing::bincode_cfg;
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    /// Encode `v` with both codecs and assert byte-equality, then round-trip ours.
    fn assert_matches_bincode<T>(v: &T)
    where
        T: Serialize + for<'d> Deserialize<'d> + PartialEq + std::fmt::Debug,
    {
        let mine = to_vec(v).unwrap();
        let theirs = bincode::serde::encode_to_vec(v, bincode_cfg()).unwrap();
        assert_eq!(mine, theirs, "encode mismatch for {v:?}");
        let back: T = from_slice(&mine).unwrap();
        assert_eq!(&back, v, "round-trip mismatch for {v:?}");
        // bincode's bytes must also decode through us.
        let cross: T = from_slice(&theirs).unwrap();
        assert_eq!(&cross, v, "cross-decode mismatch for {v:?}");
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    enum Shape {
        Unit,
        New(u64),
        Tuple(u8, String),
        Struct { a: i32, b: Vec<u8>, c: Option<f64> },
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Nested {
        id: u64,
        name: Arc<str>,
        flags: Vec<bool>,
        opt: Option<Box<Shape>>,
        map: BTreeMap<String, i64>,
        fixed: [u8; 4],
        ratio: f32,
        sig: ([u8; 32], [u8; 32]),
    }

    #[test]
    fn primitives_match_bincode() {
        assert_matches_bincode(&true);
        assert_matches_bincode(&false);
        assert_matches_bincode(&0u8);
        assert_matches_bincode(&u64::MAX);
        assert_matches_bincode(&(-1234567i64));
        assert_matches_bincode(&core::f32::consts::PI);
        assert_matches_bincode(&core::f64::consts::E);
        assert_matches_bincode(&String::from("héllo wörld"));
        assert_matches_bincode(&Option::<u32>::None);
        assert_matches_bincode(&Some(42u32));
        assert_matches_bincode(&vec![1u64, 2, 3]);
        assert_matches_bincode(&(1u8, 2u16, 3u32, 4u64));
    }

    #[test]
    fn enums_match_bincode() {
        assert_matches_bincode(&Shape::Unit);
        assert_matches_bincode(&Shape::New(0xDEAD_BEEF));
        assert_matches_bincode(&Shape::Tuple(7, "x".into()));
        assert_matches_bincode(&Shape::Struct { a: -5, b: vec![9, 8, 7], c: Some(1.5) });
        assert_matches_bincode(&Shape::Struct { a: 0, b: vec![], c: None });
    }

    #[test]
    fn nested_struct_matches_bincode() {
        let mut map = BTreeMap::new();
        map.insert("alpha".to_string(), -1i64);
        map.insert("beta".to_string(), 2i64);
        let v = Nested {
            id: 0x0102_0304_0506_0708,
            name: Arc::from("node-a:7000"),
            flags: vec![true, false, true],
            opt: Some(Box::new(Shape::New(99))),
            map,
            fixed: [0xAA, 0xBB, 0xCC, 0xDD],
            ratio: 0.625,
            sig: ([0x11; 32], [0x22; 32]),
        };
        assert_matches_bincode(&v);
    }

    #[test]
    fn from_slice_tolerates_trailing_bytes_like_bincode() {
        // bincode's decode_from_slice ignores trailing bytes; from_slice must match,
        // because `cap/` values are a Capability-prefix followed by a trailing u64
        // (CapEntry's refresh interval) that legacy/card readers decode past.
        let mut bytes = to_vec(&42u32).unwrap();
        bytes.extend_from_slice(&60_000u64.to_le_bytes()); // trailing suffix
        assert_eq!(from_slice::<u32>(&bytes).unwrap(), 42, "must decode the prefix, ignore trailing");
        // And it stays byte-equivalent to bincode's lenient decode.
        let (via_bincode, _): (u32, _) =
            bincode::serde::decode_from_slice(&bytes, bincode_cfg()).unwrap();
        assert_eq!(via_bincode, 42);

        // The real shape: a prefix struct followed by a trailing scalar (CapEntry layout).
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Prefix { a: u8, name: String, tags: Vec<u16> }
        let p = Prefix { a: 9, name: "cap".into(), tags: vec![1, 2, 3] };
        let mut buf = to_vec(&p).unwrap();
        buf.extend_from_slice(&123u64.to_le_bytes()); // CapEntry-style trailing u64
        assert_eq!(from_slice::<Prefix>(&buf).unwrap(), p,
            "struct prefix must decode while ignoring the trailing scalar");
    }

    #[test]
    fn adversarial_bytes_never_panic() {
        let mut rng = fastrand::Rng::with_seed(0xF1F1_C0DE);
        for _ in 0..20_000 {
            let len = rng.usize(0..96);
            let mut v = vec![0u8; len];
            for byte in &mut v { *byte = rng.u8(..); }
            // Decoding arbitrary bytes into a rich type must never panic/OOM.
            let _ = from_slice::<Nested>(&v);
            let _ = from_slice::<Shape>(&v);
        }
    }
}
