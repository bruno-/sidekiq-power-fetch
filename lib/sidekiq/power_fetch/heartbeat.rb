# frozen_string_literal: true

require "singleton"

module Sidekiq
  class PowerFetch
    class Heartbeat
      include Singleton

      INTERVAL = 20 # seconds
      LIFESPAN = 60 # seconds
      RETRY_DELAY = 1 # seconds

      def self.start
        raise "#{self} already started" if @started

        instance
        @started = true
      end

      def self.key(identity)
        id = identity.tr(":", "-")
        "sidekiq-power-fetch-heartbeat-#{id}"
      end

      def initialize
        # Pulse immediately to prevent a race condition where
        # PowerFertch.worker_dead? returns true in another thread.
        # `@thread` may NOT run before Sidekiq attempts to fetch jobs.
        pulse

        pulse_periodically
      end

      private

      def key
        @key ||= self.class.key(PowerFetch.identity)
      end

      def pulse
        Sidekiq.redis do |conn|
          conn.set(key, 1, ex: LIFESPAN)
        end

        Sidekiq.logger.debug("Heartbeat for #{PowerFetch.identity}")
      end

      def thread
        @thread ||= Thread.new do
          loop do
            pulse

            sleep INTERVAL
          rescue => e
            Sidekiq.logger.error("Heartbeat thread error: #{e.message}")

            sleep RETRY_DELAY
          end
        end
      end
      alias_method :pulse_periodically, :thread
    end
  end
end
