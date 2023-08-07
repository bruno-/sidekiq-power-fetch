# frozen_string_literal: true

source "https://rubygems.org"

gemspec

group :test do
  gem "rspec", "~> 3"
  gem "simplecov", require: false
  gem "stub_env", "~> 1.0"
end
