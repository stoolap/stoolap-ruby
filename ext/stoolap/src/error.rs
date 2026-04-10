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

use magnus::{
    exception::ExceptionClass,
    prelude::*,
    value::{Lazy, ReprValue},
    Error, RClass, RModule, Ruby,
};

/// Lazily-resolved `Stoolap::Error` class. The class itself is created in
/// `init()` (`module.define_error`) so by the time any binding method runs,
/// the constant is guaranteed to exist.
static ERROR_CLASS: Lazy<ExceptionClass> = Lazy::new(|ruby| {
    let module: RModule = ruby
        .class_object()
        .const_get("Stoolap")
        .expect("Stoolap module must exist before Error class is resolved");
    let class: RClass = module
        .const_get("Error")
        .expect("Stoolap::Error must be defined during init");
    // RClass -> ExceptionClass: Stoolap::Error is a subclass of StandardError.
    ExceptionClass::from_value(class.as_value()).expect("Stoolap::Error must be an exception class")
});

/// Build a Magnus `Error` from a Stoolap engine error.
pub fn to_magnus(err: stoolap::Error) -> Error {
    let ruby = Ruby::get().expect("must hold the Ruby VM lock");
    Error::new(ruby.get_inner(&ERROR_CLASS), err.to_string())
}

/// Build a Magnus `Error` from a free-form message.
pub fn raise<S: Into<String>>(msg: S) -> Error {
    let ruby = Ruby::get().expect("must hold the Ruby VM lock");
    Error::new(ruby.get_inner(&ERROR_CLASS), msg.into())
}

/// Build a `TypeError` for invalid Ruby types.
pub fn type_error<S: Into<String>>(msg: S) -> Error {
    let ruby = Ruby::get().expect("must hold the Ruby VM lock");
    Error::new(ruby.exception_type_error(), msg.into())
}
