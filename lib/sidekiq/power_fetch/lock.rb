# frozen_string_literal: true

module Sidekiq
  class PowerFetch
    class Lock
      DEFAULT_INTERVAL = 120 # 2 minutes
      DEFAULT_RECOVER = 3600 # 1 hour
      KEY = "sidekiq-power-fetch-lock"

      def initialize(config)
        @config = config
        @taken_at = 0

        # Sidekiq processes should attempt recovery every 2 minutes.
        @interval = @config[:power_fetch_lock] || DEFAULT_INTERVAL
        # Perform recovery at most every 1 hour.
        @recover = @config[:power_fetch_recover] || DEFAULT_RECOVER
      end

      def lock
        return unless take?

        @taken_at = time
        @config.redis do |conn|
          conn.set(KEY, 1, nx: true, ex: @recover)
        end
      end

      private

      def take?
        time - @taken_at > @interval
      end

      def time
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end
    end
  end
end
