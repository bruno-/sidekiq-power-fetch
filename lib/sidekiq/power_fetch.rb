# frozen_string_literal: true

require_relative "power_fetch/heartbeat"
require_relative "power_fetch/recover"
require_relative "power_fetch/unit_of_work"

module Sidekiq
  class PowerFetch
    WORKING_QUEUE_PREFIX = "working"

    # How much time a job can be interrupted
    DEFAULT_MAX_RETRIES_AFTER_INTERRUPTION = 3

    # Regexes for matching working queue keys
    WORKING_QUEUE_REGEX = /#{WORKING_QUEUE_PREFIX}:(queue:.*):([^:]*:[0-9]*:[0-9a-f]*)\z/.freeze
    LEGACY_WORKING_QUEUE_REGEX = /#{WORKING_QUEUE_PREFIX}:(queue:.*):([^:]*:[0-9]*)\z/.freeze

    # For reliable fetch we don't use Redis' blocking operations so
    # we inject a regular sleep into the loop.
    IDLE_TIMEOUT = 5 # seconds

    def self.setup!(config)
      config[:fetch] = new(config)
      config.logger.info("Sidekiq power fetch activated!")

      Heartbeat.start
    end

    def self.identity
      @identity ||= begin
        hostname = ENV["DYNO"] || Socket.gethostname
        pid = Process.pid
        process_nonce = SecureRandom.hex(6)

        "#{hostname}:#{pid}:#{process_nonce}"
      end
    end

    def self.worker_dead?(identity, conn)
      !conn.get(Heartbeat.key(identity))
    end

    def self.working_queue_name(queue)
      "#{WORKING_QUEUE_PREFIX}:#{queue}:#{identity}"
    end

    def initialize(config)
      raise ArgumentError, "missing queue list" unless config.queues

      @config = config
      @recover = Recover.new(@config)

      @strictly_ordered_queues = (@config.queues.size == config.queues.uniq.size)
      @queues = config.queues.map { |q| "queue:#{q}" }
      if @strictly_ordered_queues
        @queues.uniq!
      end
    end

    def retrieve_work
      if @recover.lock
        @recover.call
      end

      queues_list = @strictly_ordered_queues ? @queues : @queues.shuffle

      queues_list.each do |queue|
        work = @config.redis do |conn|
          conn.rpoplpush(queue, self.class.working_queue_name(queue))
        end

        return UnitOfWork.new(queue, work) if work
      end

      # We didn't find a job in any of the configured queues. Let's sleep a bit
      # to avoid uselessly burning too much CPU
      sleep(IDLE_TIMEOUT)

      nil
    end

    # Called by sidekiq on "hard shutdown": when shutdown is reached, and there
    # are still busy threads. The threads are shutdown, but their jobs are
    # requeued.
    # https://github.com/sidekiq/sidekiq/blob/323a5cfaefdde20588f5ffdf0124691db83fd315/lib/sidekiq/manager.rb#L107
    def bulk_requeue(inprogress, _options)
      return if inprogress.empty?

      @config.redis do |conn|
        inprogress.each do |unit_of_work|
          conn.multi do |multi|
            msg = Sidekiq.load_json(unit_of_work.job)
            @recover.requeue_job(unit_of_work.queue, msg, multi)

            multi.lrem(self.class.working_queue_name(unit_of_work.queue), 1, unit_of_work.job)
          end
        end
      end
    rescue => e
      @config.logger.warn("Failed to requeue #{inprogress.size} jobs: #{e.message}")
    end
  end
end
