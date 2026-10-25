// Copyright 2025 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cell::RefCell;
use std::os::raw::c_int;

use chrono::{TimeZone, Utc};
use magnus::{
    prelude::*,
    r_hash::ForEach,
    rb_sys::{AsRawValue, FromRawValue},
    value::{Lazy, ReprValue},
    Error, Float, Integer, RArray, RClass, RHash, RModule, RString, Ruby, Symbol, TryConvert,
    Value,
};

use rb_sys::timespec;
use stoolap::api::{NamedParams, ParamVec};
use stoolap::core::Value as SValue;

use crate::error::{raise, type_error};

/// Cached `Time` class. Lazy-initialised on first use; subsequent calls
/// are a direct pointer load, avoiding `const_get("Time")` on every
/// timestamp parameter.
static TIME_CLASS: Lazy<RClass> = Lazy::new(|ruby| {
    ruby.class_object()
        .const_get("Time")
        .expect("Ruby core class Time must exist")
});

/// Cached `JSON` module used to serialise Hash/Array params.
/// `lib/stoolap.rb` does `require "json"` so the constant is
/// guaranteed to exist by the time any binding method runs. The
/// `.expect` is a last-resort guard, not a normal error path.
static JSON_MODULE: Lazy<RModule> = Lazy::new(|ruby| {
    ruby.class_object()
        .const_get("JSON")
        .expect("JSON constant must exist (require \"json\" should have loaded it)")
});

/// A vector of f32 values for similarity search.
///
/// Wraps a list of floats so that Stoolap stores them as a native VECTOR
/// rather than a JSON array.
///
/// @example
///   v = Stoolap::Vector.new([0.1, 0.2, 0.3])
///   db.execute("INSERT INTO t (embedding) VALUES ($1)", [v])
#[magnus::wrap(class = "Stoolap::Vector", free_immediately, size)]
pub struct Vector {
    pub data: RefCell<Vec<f32>>,
}

impl Vector {
    pub fn new(data: RArray) -> Result<Self, Error> {
        let mut floats: Vec<f32> = Vec::with_capacity(data.len());
        for item in data.into_iter() {
            let f: f64 = match TryConvert::try_convert(item) {
                Ok(v) => v,
                Err(_) => {
                    return Err(type_error("Vector elements must be numeric"));
                }
            };
            floats.push(f as f32);
        }
        Ok(Self {
            data: RefCell::new(floats),
        })
    }

    pub fn to_a(&self) -> Vec<f64> {
        self.data.borrow().iter().map(|f| *f as f64).collect()
    }

    pub fn length(&self) -> usize {
        self.data.borrow().len()
    }

    pub fn inspect(&self) -> String {
        let v = self.data.borrow();
        let parts: Vec<String> = v.iter().map(|f| format!("{}", f)).collect();
        format!("#<Stoolap::Vector [{}]>", parts.join(", "))
    }

    /// Internal: snapshot the contained data into a fresh Vec.
    pub fn snapshot(&self) -> Vec<f32> {
        self.data.borrow().clone()
    }
}

/// Parsed bind parameters from Ruby.
pub enum BindParams {
    Positional(ParamVec),
    Named(NamedParams),
}

/// Convert a Ruby Value into a Stoolap Value.
pub fn ruby_to_value(val: Value) -> Result<SValue, Error> {
    let ruby = magnus::Ruby::get().expect("must hold the Ruby VM lock");

    if val.is_nil() {
        return Ok(SValue::null_unknown());
    }

    // Boolean: true and false are immediates, so compare them directly
    // instead of looking up the class of every parameter.
    if val.as_raw() == ruby.qtrue().as_raw() {
        return Ok(SValue::Boolean(true));
    }
    if val.as_raw() == ruby.qfalse().as_raw() {
        return Ok(SValue::Boolean(false));
    }

    // Integer
    if let Some(i) = Integer::from_value(val) {
        let n: i64 = i.to_i64()?;
        return Ok(SValue::Integer(n));
    }

    // Float
    if let Some(f) = Float::from_value(val) {
        return Ok(SValue::Float(f.to_f64()));
    }

    // String
    if let Some(s) = RString::from_value(val) {
        let owned = unsafe { s.as_str()?.to_owned() };
        return Ok(SValue::text(owned));
    }

    // Symbol -> string
    if let Some(sym) = Symbol::from_value(val) {
        return Ok(SValue::text(sym.name()?.into_owned()));
    }

    // Stoolap::Vector wrapper
    if let Ok(v) = <&Vector>::try_convert(val) {
        return Ok(SValue::vector(v.snapshot()));
    }

    // Time -> Timestamp (UTC)
    let time_class = ruby.get_inner(&TIME_CLASS);
    if val.is_kind_of(time_class) {
        // `rb_time_timespec` raises for a Time outside the system range, so
        // run it under `protect` to get an `Error` instead of a longjmp.
        let mut spec = timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        magnus::rb_sys::protect(|| {
            spec = unsafe { rb_sys::rb_time_timespec(val.as_raw()) };
            ruby.qnil().as_raw()
        })?;
        let dt = Utc
            .timestamp_opt(spec.tv_sec as i64, spec.tv_nsec as u32)
            .single()
            .ok_or_else(|| raise("invalid Time value"))?;
        return Ok(SValue::Timestamp(dt));
    }

    // Array / Hash -> JSON
    if RArray::from_value(val).is_some() || RHash::from_value(val).is_some() {
        let json_module = ruby.get_inner(&JSON_MODULE);
        let dumped: RString = json_module.funcall("dump", (val,))?;
        let s = unsafe { dumped.as_str()?.to_owned() };
        return Ok(SValue::json(s));
    }

    Err(type_error(format!(
        "Unsupported parameter type: {}",
        val.class().inspect()
    )))
}

/// Parse Ruby params (Array, Hash, or nil) into BindParams.
pub fn parse_params(params: Option<Value>) -> Result<BindParams, Error> {
    let params = match params {
        None => return Ok(BindParams::Positional(ParamVec::new())),
        Some(v) if v.is_nil() => return Ok(BindParams::Positional(ParamVec::new())),
        Some(v) => v,
    };

    // Hash -> named params
    if let Some(hash) = RHash::from_value(params) {
        let mut named = NamedParams::new();
        let mut err: Option<Error> = None;
        hash.foreach(|key: Value, val: Value| {
            // Extract the key as `&str` without an intermediate owned String.
            // Symbols return a `Cow<str>` from `name()`; RStrings expose their
            // bytes via `unsafe as_str` (valid while the Hash holds the key).
            // A single `to_string` at `named.insert` is the only allocation.
            let rstring = RString::from_value(key);
            let symbol = Symbol::from_value(key);

            let raw: &str = if let Some(sym) = symbol {
                match sym.name() {
                    Ok(std::borrow::Cow::Borrowed(s)) => s,
                    // `sym.name()` returns Borrowed for pinned symbols in 3.x
                    // so the Owned branch should be unreachable in practice.
                    Ok(std::borrow::Cow::Owned(_)) => {
                        err = Some(type_error("dynamic symbol key not supported"));
                        return Ok(ForEach::Stop);
                    }
                    Err(e) => {
                        err = Some(e);
                        return Ok(ForEach::Stop);
                    }
                }
            } else if let Some(ref s) = rstring {
                match unsafe { s.as_str() } {
                    Ok(s) => s,
                    Err(e) => {
                        err = Some(e);
                        return Ok(ForEach::Stop);
                    }
                }
            } else {
                err = Some(type_error("named parameter keys must be Symbol or String"));
                return Ok(ForEach::Stop);
            };

            let stripped = raw.trim_start_matches(&[':', '@', '$'][..]);

            match ruby_to_value(val) {
                Ok(v) => {
                    named.insert(stripped.to_string(), v);
                    Ok(ForEach::Continue)
                }
                Err(e) => {
                    err = Some(e);
                    Ok(ForEach::Stop)
                }
            }
        })?;
        if let Some(e) = err {
            return Err(e);
        }
        return Ok(BindParams::Named(named));
    }

    // Array -> positional
    if let Some(arr) = RArray::from_value(params) {
        let mut values = ParamVec::new();
        for item in arr.into_iter() {
            values.push(ruby_to_value(item)?);
        }
        return Ok(BindParams::Positional(values));
    }

    Err(type_error("Parameters must be an Array, Hash, or nil"))
}

/// `rb_time_timespec_new` offset that makes a UTC Time (`INT_MAX - 1`).
const UTC_OFFSET: c_int = c_int::MAX - 1;

/// Convert a Stoolap Value to a Ruby Value.
pub fn value_to_ruby(ruby: &Ruby, val: &SValue) -> Result<Value, Error> {
    match val {
        SValue::Null(_) => Ok(ruby.qnil().as_value()),
        SValue::Boolean(b) => Ok(if *b {
            ruby.qtrue().as_value()
        } else {
            ruby.qfalse().as_value()
        }),
        SValue::Integer(i) => Ok(ruby.integer_from_i64(*i).as_value()),
        SValue::Float(f) => Ok(ruby.float_from_f64(*f).as_value()),
        SValue::Text(s) => Ok(ruby.str_new(s.as_str()).as_value()),
        SValue::Timestamp(ts) => {
            // One C call builds the UTC Time directly, instead of
            // `Time.at(secs, nsecs, :nsec).utc` through two method calls.
            let spec = timespec {
                tv_sec: ts.timestamp() as _,
                tv_nsec: ts.timestamp_subsec_nanos() as _,
            };
            Ok(unsafe { Value::from_raw(rb_sys::rb_time_timespec_new(&spec, UTC_OFFSET)) })
        }
        SValue::Extension(_) => {
            if let Some(floats) = val.as_vector_f32() {
                let arr = ruby.ary_new_capa(floats.len());
                for f in floats.iter() {
                    arr.push(*f as f64)?;
                }
                Ok(arr.as_value())
            } else if let Some(s) = val.as_json() {
                Ok(ruby.str_new(s).as_value())
            } else {
                Ok(ruby.str_new(&format!("{}", val)).as_value())
            }
        }
    }
}
