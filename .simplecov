# frozen_string_literal: true

# Loaded automatically by `require "simplecov"`. Kept in sync with
# test/test_helper.rb so both ad-hoc runs and rake task use the same config.
SimpleCov.start do
  minimum_coverage 80
  command_name "Unit Tests"
  add_filter "/test/"
  add_filter "/ext/"
  add_filter "/tmp/"
  add_filter "/target/"
  # version.rb is loaded by the gemspec via Bundler.setup BEFORE SimpleCov
  # can start, so its three constant-declaration lines cannot be
  # instrumented. Filter it out instead of skewing the metric.
  add_filter "lib/stoolap/version.rb"
  add_group "Library", "lib"
  track_files "lib/**/*.rb"
end
