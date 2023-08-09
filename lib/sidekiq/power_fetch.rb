# frozen_string_literal: true

require_relative "power_fetch/heartbeat"
require_relative "power_fetch/unit_of_work"
# "recover" is required at the bottom of the file, after
# PowerFetch::WORKING_QUEUE_PREFIX constant is defined.

module Sidekiq
  class PowerFetch
    WORKING_QUEUE_PREFIX = "working"

    # We don't use Redis' blocking operations for fetch so
    # we inject a regular sleep into the loop.
    IDLE_TIMEOUT = 5 # seconds

    def self.identity
      @identity ||= begin
        hostname = ENV["DYNO"] || Socket.gethostname
        pid = ::Process.pid
        process_nonce = SecureRandom.hex(6)

        "#{hostname}:#{pid}:#{process_nonce}"
      end
    end

    def self.working_queue_name(queue)
      "#{WORKING_QUEUE_PREFIX}:#{queue}:#{identity}"
    end

    def initialize(capsule)
      raise ArgumentError, "missing queue list" unless capsule.queues
      @capsule = capsule
      @strictly_ordered_queues = capsule.mode == :strict
      @queues = @capsule.queues.map { |q| "queue:#{q}" }
      @queues.uniq! if @strictly_ordered_queues

      @recover = Recover.new(@capsule)
      @capsule.logger.info("[PowerFetch] Activated!")
    end

    def retrieve_work
      if @recover.lock
        @recover.call
      end

      queues_list = @strictly_ordered_queues ? @queues : @queues.shuffle

      queues_list.each do |queue|
        work = @capsule.redis do |conn|
          # Can't use 'blmove' here: empty blocked queue would then block
          # other, potentially non-empty, queues.
          conn.lmove(queue, self.class.working_queue_name(queue), :right, :left)
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
    def bulk_requeue(inprogress)
      return if inprogress.empty?

      @capsule.redis do |conn|
        inprogress.each do |unit_of_work|
          conn.multi do |multi|
            msg = Sidekiq.load_json(unit_of_work.job)
            @recover.requeue_job(unit_of_work.queue, msg, multi)

            multi.lrem(self.class.working_queue_name(unit_of_work.queue), 1, unit_of_work.job)
          end
        end
      end
    rescue => e
      @capsule.logger.warn("[PowerFetch] Failed to requeue #{inprogress.size} jobs: #{e.message}")
    end
  end
end

require_relative "power_fetch/recover"
