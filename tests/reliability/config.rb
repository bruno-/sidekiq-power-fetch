# frozen_string_literal: true

require_relative "../../lib/sidekiq-power-fetch"
require_relative "worker"

REDIS_FINISHED_LIST = "power-fetch-finished-jids"

NUMBER_OF_WORKERS = ENV["NUMBER_OF_WORKERS"] || 10
NUMBER_OF_JOBS = ENV["NUMBER_OF_JOBS"] || 1000
TEST_RECOVER = 20
TEST_LOCK = 5
WAIT_CLEANUP = TEST_RECOVER +
               TEST_LOCK +
               Sidekiq::PowerFetch::Heartbeat::LIFESPAN

Sidekiq.configure_server do |config|
  # We need to override these parameters to not wait too long
  # The default values are good for production use only
  config[:power_fetch_recover] = TEST_RECOVER
  config[:power_fetch_lock] = TEST_LOCK
  config.redis = {
    db: 11
  }
end
