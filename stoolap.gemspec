# frozen_string_literal: true

require_relative "lib/stoolap/version"

Gem::Specification.new do |spec|
  spec.name = "stoolap"
  spec.version = Stoolap::VERSION
  spec.authors = ["Stoolap Contributors"]
  spec.license = "Apache-2.0"

  spec.summary = "High-performance embedded SQL database for Ruby"
  spec.description = "Native Ruby driver for the Stoolap embedded SQL database. " \
                     "Built with Magnus + rb-sys for zero-FFI Rust performance."
  spec.homepage = "https://stoolap.io"
  spec.metadata["source_code_uri"] = "https://github.com/stoolap/stoolap-ruby"
  spec.metadata["documentation_uri"] = "https://stoolap.io/docs/drivers/ruby/"
  spec.metadata["bug_tracker_uri"] = "https://github.com/stoolap/stoolap-ruby/issues"

  spec.required_ruby_version = ">= 3.3.0"
  spec.required_rubygems_version = ">= 3.3.11"

  # Explicit allow-list so we never ship the local cargo `target/` build
  # cache, nor any other scratch file, inside the published gem.
  spec.files = Dir[
    "lib/**/*.rb",
    "ext/stoolap/src/**/*.rs",
    "ext/stoolap/Cargo.toml",
    "ext/stoolap/extconf.rb",
    "Cargo.toml",
    "Cargo.lock",
    "LICENSE",
    "README.md"
  ]
  spec.require_paths = ["lib"]
  spec.extensions = ["ext/stoolap/extconf.rb"]

  spec.add_dependency "rb_sys", "~> 0.9.91"

  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rake-compiler", "~> 1.2"
  spec.add_development_dependency "minitest", ">= 5.20"
  spec.add_development_dependency "simplecov", "~> 0.22"
  spec.add_development_dependency "sqlite3", "~> 2.0"
end
