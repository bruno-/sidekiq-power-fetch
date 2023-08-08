# frozen_string_literal: true

require_relative "lock"
require_relative "interrupted_set"

module Sidekiq
  class PowerFetch
    class Recover
      # Defines the COUNT parameter that will be passed to Redis SCAN command
      SCAN_COUNT = 1000

      # How much time a job can be interrupted
      RECOVERIES = 3

      # Regexes for matching working queue keys
      WORKING_QUEUE_REGEX = /\A#{WORKING_QUEUE_PREFIX}:(queue:.*):([^:]*:[0-9]*:[0-9a-f]*)\z/

      def initialize(config)
        @config = config
        @lock = Lock.new(@config)
        @interrupted_set = InterruptedSet.new
        @recoveries = @config[:power_fetch_recoveries] || RECOVERIES
      end

      def lock
        @lock.lock
      end

      # Detect "old" jobs and requeue them because the worker they were assigned
      # to probably failed miserably.
      def call
        @config.logger.info("[PowerFetch] Recovering working queues")

        @config.redis do |conn|
          conn.scan(
            match: "#{WORKING_QUEUE_PREFIX}:queue:*",
            count: SCAN_COUNT
          ) do |key|
            # Identity format is "{hostname}:{pid}:{randomhex}
            # Queue names may also have colons (namespaced).
            # Expressing this in a single regex is unreadable
            original_queue, identity = key.scan(WORKING_QUEUE_REGEX).flatten

            next if original_queue.nil? || identity.nil?

            if worker_dead?(identity, conn)
              recover_working_queue!(original_queue, key)
            end
          end
        end
      end

      # If you want this method to be run in a scope of multi connection
      # you need to pass it
      def requeue_job(queue, msg, conn)
        with_connection(conn) do |conn|
          conn.lpush(queue, Sidekiq.dump_json(msg))
        end

        @config.logger.info(
          "[PowerFetch] Pushed job #{msg["jid"]} back to queue '#{queue}'"
        )
      end

      private

      def recover_working_queue!(original_queue, working_queue)
        @config.redis do |conn|
          while job = conn.rpop(working_queue)
            preprocess_interrupted_job(job, original_queue)
          end
        end
      end

      def preprocess_interrupted_job(job, queue, conn = nil)
        msg = Sidekiq.load_json(job)
        msg["interrupted_count"] = msg["interrupted_count"].to_i + 1

        if interruption_exhausted?(msg)
          send_to_quarantine(msg, conn)
        else
          requeue_job(queue, msg, conn)
        end
      end

      def interruption_exhausted?(msg)
        return false if @recoveries < 0

        msg["interrupted_count"].to_i >= @recoveries
      end

      def send_to_quarantine(msg, multi_connection = nil)
        @config.logger.warn(
          "[PowerFetch]: adding dead #{msg["class"]} job #{msg["jid"]} " \
          "to interrupted queue"
        )

        job = Sidekiq.dump_json(msg)
        @interrupted_set.put(job, connection: multi_connection)
      end

      # Yield block with an existing connection or creates another one
      def with_connection(conn)
        return yield(conn) if conn

        @config.redis { |redis_conn| yield(redis_conn) }
      end

      def worker_dead?(identity, conn)
        !conn.get(Heartbeat.key(identity))
      end
    end
  end
end
