# frozen_string_literal: true

require_relative "lock"

module Sidekiq
  class PowerFetch
    class Recover
      # Defines the COUNT parameter that will be passed to Redis SCAN command
      SCAN_COUNT = 1000

      # How much time a job can be interrupted
      RECOVERIES = 3

      # Regexes for matching working queue keys
      WORKING_QUEUE_REGEX = /\A#{WORKING_QUEUE_PREFIX}:(queue:.*):([^:]*:[0-9]*:[0-9a-f]*)\z/

      def initialize(capsule)
        @capsule = capsule
        @lock = Lock.new(@capsule)
        @recoveries = @capsule.lookup(:power_fetch_recoveries) || RECOVERIES
      end

      def lock
        @lock.lock
      end

      # Detect "old" jobs and requeue them because the worker they were assigned
      # to probably failed miserably.
      def call
        @capsule.logger.info("[PowerFetch] Recovering working queues")

        @capsule.redis do |conn|
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

        @capsule.logger.info(
          "[PowerFetch] Pushed job #{msg["jid"]} back to queue '#{queue}'"
        )
      end

      private

      def recover_working_queue!(original_queue, working_queue)
        @capsule.redis do |conn|
          while job = conn.rpop(working_queue)
            preprocess_interrupted_job(job, original_queue)
          end
        end
      end

      def preprocess_interrupted_job(job, queue, conn = nil)
        msg = Sidekiq.load_json(job)
        msg["interrupted_count"] = msg["interrupted_count"].to_i + 1

        if interruption_exhausted?(msg)
          @capsule.logger.warn(
            "[PowerFetch] Deleted job #{msg["class"]} jid #{msg["jid"]}, " \
            "it was recovered too many times"
          )
        else
          requeue_job(queue, msg, conn)
        end
      end

      def interruption_exhausted?(msg)
        return false if @recoveries < 0

        msg["interrupted_count"].to_i >= @recoveries
      end

      # Yield block with an existing connection or creates another one
      def with_connection(conn)
        return yield(conn) if conn

        @capsule.redis { |redis_conn| yield(redis_conn) }
      end

      def worker_dead?(identity, conn)
        !conn.get(Heartbeat.key(identity))
      end
    end
  end
end
