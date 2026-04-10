# frozen_string_literal: true

require "bundler/gem_tasks"
require "rake/testtask"
require "rb_sys/extensiontask"

GEMSPEC = Gem::Specification.load("stoolap.gemspec")

RbSys::ExtensionTask.new("stoolap", GEMSPEC) do |ext|
  ext.lib_dir = "lib/stoolap"
end

Rake::TestTask.new(test: :compile) do |t|
  t.libs << "test"
  t.libs << "lib"
  t.test_files = FileList["test/test_*.rb"]
  t.warning = false
  # Load SimpleCov before anything else so every library file, including
  # lib/stoolap/version.rb (pulled in by the gemspec), gets instrumented.
  t.ruby_opts = ["-rsimplecov"]
end

task default: :test
