require_relative '../lib/sidekiq/reliable_fetcher'
require_relative 'worker'

REDIS_FINISHED_LIST = 'reliable-fetcher-finished-jids'

NUMBER_OF_WORKERS = ENV['NUMBER_OF_WORKERS'] || 10
NUMBER_OF_JOBS = ENV['NUMBER_OF_JOBS'] || 1000
JOB_FETCHER = (ENV['JOB_FETCHER'] || :semi).to_sym # :base, :semi, :reliable
TEST_CLEANUP_INTERVAL = 20
TEST_LEASE_INTERVAL = 5
WAIT_CLEANUP = TEST_CLEANUP_INTERVAL + TEST_LEASE_INTERVAL + Sidekiq::ReliableFetcher::HEARTBEAT_LIFESPAN

Sidekiq.configure_server do |config|
  if %i[semi reliable].include?(JOB_FETCHER)
    Sidekiq::ReliableFetcher.setup_reliable_fetch!(config)
  end

  config.options[:semi_reliable_fetch] = true if JOB_FETCHER == :semi

  # We need to override thse parameters to not wait to long
  # The default values are good for production use only
  # These will be ignored for :base
  config.options[:cleanup_interval] = TEST_CLEANUP_INTERVAL
  config.options[:lease_interval] = TEST_LEASE_INTERVAL
end
