# frozen_string_literal: true

module Sidekiq
  class PowerFetch
    class Heartbeat
      LIFESPAN = 60 # seconds

      def self.start(config)
        new(config)
      end

      def self.started?
        @started
      end

      def self.started=(value)
        @started = value
      end

      def initialize(config)
        raise "#{self.class} already started" if self.class.started?

        @config = config
        @config.on(:heartbeat) do
          pulse
        end
        self.class.started = true
      end

      def self.key(identity)
        id = identity.tr(":", "-")
        "sidekiq-power-fetch-heartbeat-#{id}"
      end

      def pulse
        @config.redis do |conn|
          conn.set(key, 1, ex: LIFESPAN)
        end

        @config.logger.debug("[PowerFetch] Heartbeat for #{PowerFetch.identity}")
      end

      private

      def key
        @key ||= self.class.key(PowerFetch.identity)
      end
    end
  end
end
