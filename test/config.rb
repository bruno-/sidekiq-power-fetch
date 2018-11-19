require_relative '../lib/sidekiq/reliable_fetcher'
require_relative 'worker'

NUMBER_OF_WORKERS = ENV['NUMBER_OF_WORKERS'] || 10
NUMBER_OF_JOBS = ENV['NUMBER_OF_JOBS'] || 1000
JOB_FETCHER = (ENV['JOB_FETCHER'] || :semi).to_sym # :base, :semi, :reliable

# Should be enough time for clean up, should be >= (take_lease_time_interval + cleanup_interval)
WAIT_CLEANUP = 65

Sidekiq.configure_server do |config|
  if %i[semi reliable].include?(JOB_FETCHER)
    Sidekiq::ReliableFetcher.setup_reliable_fetch!(config)
  end

  config.options[:semi_reliable_fetch] = true if JOB_FETCHER == :semi

  # We need to override thse parameters to not wait to long
  # The default values are good for production use only
  # These will be ignored for :base
  config.options[:cleanup_interval] = 60
  config.options[:lease_time_interval] = 5
end
