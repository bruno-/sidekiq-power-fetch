# frozen_string_literal: true

source "https://rubygems.org"

git_source(:github) { |repo_name| "https://github.com/#{repo_name}" }

group :test do
  gem "rspec", '~> 3'
  gem "pry"
  gem "sidekiq", ENV['SIDEKIQ_VERSION_FOR_TESTS'] || '~> 6.0'
  gem 'simplecov', require: false
end
