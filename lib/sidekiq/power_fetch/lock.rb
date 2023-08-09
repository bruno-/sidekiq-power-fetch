# frozen_string_literal: true

module Sidekiq
  class PowerFetch
    class Lock
      DEFAULT_INTERVAL = 120 # 2 minutes
      DEFAULT_RECOVER = 3600 # 1 hour
      KEY = "sidekiq-power-fetch-lock"

      def initialize(capsule)
        @capsule = capsule
        @taken_at = 0

        # Sidekiq processes should attempt recovery every 2 minutes.
        @interval = @capsule.lookup(:power_fetch_lock) || DEFAULT_INTERVAL
        # Perform recovery at most every 1 hour.
        @recover = @capsule.lookup(:power_fetch_recover) || DEFAULT_RECOVER
      end

      def lock
        return unless take?

        @taken_at = time
        @capsule.redis do |conn|
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
