# frozen_string_literal: true

require_relative "../../lib/sidekiq-power-fetch"
require_relative "worker"

TEST_RECOVER = 20
TEST_LOCK = 5

Sidekiq.configure_server do |config|
  # We need to override these parameters to not wait too long.
  # The default values are good for production use only.
  config[:power_fetch_recover] = TEST_RECOVER
  config[:power_fetch_lock] = TEST_LOCK
  config.redis = {db: 12}
end

Sidekiq.configure_client do |config|
  config.redis = {db: 12}
end
