# frozen_string_literal: true

# SimpleCov is started up-front so it can instrument all library files
# before Minitest or the gemspec touches them. The configuration lives
# in the top-level `.simplecov` file.
require "simplecov"

require "minitest/autorun"
require "stoolap"
