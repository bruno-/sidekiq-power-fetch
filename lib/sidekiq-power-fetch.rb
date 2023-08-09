# frozen_string_literal: true

require "sidekiq"
require "sidekiq/api"

require_relative "sidekiq/power_fetch"

Sidekiq.configure_server do |config|
  config[:fetch_class] = Sidekiq::PowerFetch

  # There's only one Heartbeat per process.
  # Starting it as soon as possible.
  Sidekiq::PowerFetch::Heartbeat.start(config)
end
