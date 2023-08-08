# frozen_string_literal: true

require_relative "lock"
require_relative "interrupted_set"

module Sidekiq
  class PowerFetch
    class Recover
      # Defines the COUNT parameter that will be passed to Redis SCAN command
      SCAN_COUNT = 1000

      def initialize(config)
        @config = config
        @lock = Lock.new(@config)
        @interrupted_set = InterruptedSet.new
      end

      def lock
        @lock.lock
      end

      # Detect "old" jobs and requeue them because the worker they were assigned
      # to probably failed miserably.
      def call
        @config.logger.info("Cleaning working queues")

        @config.redis do |conn|
          conn.scan_each(match: "#{WORKING_QUEUE_PREFIX}:queue:*", count: SCAN_COUNT) do |key|
            original_queue, identity = extract_queue_and_identity(key)

            next if original_queue.nil? || identity.nil?

            clean_working_queue!(original_queue, key) if self.class.worker_dead?(identity, conn)
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
          message: "Pushed job #{msg["jid"]} back to queue #{queue}",
          jid: msg["jid"],
          queue: queue
        )
      end

      private

      def clean_working_queue!(original_queue, working_queue)
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

      def extract_queue_and_identity(key)
        # Identity format is "{hostname}:{pid}:{randomhex}
        # Queue names may also have colons (namespaced).
        # Expressing this in a single regex is unreadable

        # Test the newer expected format first, only checking the older if necessary
        original_queue, identity = key.scan(WORKING_QUEUE_REGEX).flatten
        return original_queue, identity unless original_queue.nil? || identity.nil?

        key.scan(LEGACY_WORKING_QUEUE_REGEX).flatten
      end

      def interruption_exhausted?(msg)
        return false if max_retries_after_interruption(msg["class"]) < 0

        msg["interrupted_count"].to_i >= max_retries_after_interruption(msg["class"])
      end

      # TODO: Delete method.
      def max_retries_after_interruption(worker_class)
        max_retries_after_interruption = nil

        max_retries_after_interruption ||= begin
          Object.const_get(worker_class).sidekiq_options[:max_retries_after_interruption]
        rescue NameError
        end

        max_retries_after_interruption ||= @config[:max_retries_after_interruption]
        max_retries_after_interruption ||= DEFAULT_MAX_RETRIES_AFTER_INTERRUPTION
        max_retries_after_interruption
      end

      def send_to_quarantine(msg, multi_connection = nil)
        @config.logger.warn(
          class: msg["class"],
          jid: msg["jid"],
          message: %(Sidekiq PowerFetch: adding dead #{msg["class"]} job #{msg["jid"]} to interrupted queue)
        )

        job = Sidekiq.dump_json(msg)
        @interrupted_set.put(job, connection: multi_connection)
      end

      # Yield block with an existing connection or creates another one
      def with_connection(conn)
        return yield(conn) if conn

        @config.redis { |redis_conn| yield(redis_conn) }
      end
    end
  end
end
