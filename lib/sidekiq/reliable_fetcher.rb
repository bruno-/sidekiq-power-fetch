# frozen_string_literal: true
module Sidekiq
  class ReliableFetcher
    WORKING_QUEUE            = 'working'
    DEFAULT_DEAD_AFTER       = 60 * 60 * 24 # 24 hours
    CLEANUP_INTERVAL         = 60 * 60 # 1 hour

    # TAKE_LEASE_TIME_INTERVAL defines how often we try to take a lease
    # to not flood our Redis server with SET requests
    TAKE_LEASE_TIME_INTERVAL = 2 * 60
    IDLE_TIMEOUT             = 5 # seconds

    UnitOfWork = Struct.new(:queue, :job) do
      def acknowledge
        # NOTE LREM is O(n), so depending on the type of jobs and their average
        # duration, another data structure might be more suited.
        # But as there should not be too much jobs in this queue in the same time,
        # it's probably ok.
        Sidekiq.redis { |conn| conn.lrem("#{queue}:#{WORKING_QUEUE}", 1, job) }
      end

      def queue_name
        queue.sub(/.*queue:/, '')
      end

      def requeue
        Sidekiq.redis do |conn|
          conn.pipelined do
            conn.lpush("queue:#{queue_name}", job)
            conn.lrem("queue:#{queue_name}:#{WORKING_QUEUE}", 1, job)
          end
        end
      end
    end

    def self.setup_reliable_fetch!(config)
      config.options[:fetch] = Sidekiq::ReliableFetcher

      Sidekiq.logger.info { "GitLab reliable fetch activated!" }
    end

    def initialize(options)
      queues = options[:queues].map { |q| "queue:#{q}" }

      @unique_queues = queues.uniq
      @queues_iterator = queues.shuffle.cycle
      @queues_size  = queues.size

      @consider_dead_after = options[:consider_dead_after] || DEFAULT_DEAD_AFTER
      @cleanup_interval = options[:cleanup_interval] || CLEANUP_INTERVAL
      @take_lease_time_interval = options[:take_lease_time_interval]  || TAKE_LEASE_TIME_INTERVAL
      @last_try_to_take_lease_at = 0
    end

    def retrieve_work
      clean_working_queues!

      @queues_size.times do
        queue = @queues_iterator.next
        work = Sidekiq.redis { |conn| conn.rpoplpush(queue, "#{queue}:#{WORKING_QUEUE}") }

        if work
          return UnitOfWork.new(queue, work)
        end
      end

      # We didn't find a job in any of the configured queues. Let's sleep a bit
      # to avoid uselessly burning too much CPU
      sleep(IDLE_TIMEOUT)

      nil
    end

    # By leaving this as a class method, it can be pluggable and used by the Manager actor. Making it
    # an instance method will make it async to the Fetcher actor
    def self.bulk_requeue(inprogress, options)
      return if inprogress.empty?

      Sidekiq.logger.debug { "Re-queueing terminated jobs" }

      Sidekiq.redis do |conn|
        conn.pipelined do
          inprogress.each do |unit_of_work|
            conn.lpush("#{unit_of_work.queue}", unit_of_work.job)
            conn.lrem("#{unit_of_work.queue}:#{WORKING_QUEUE}", 1, unit_of_work.job)
          end
        end
      end

      Sidekiq.logger.info("Pushed #{inprogress.size} jobs back to Redis")
    rescue => ex
      Sidekiq.logger.warn("Failed to requeue #{inprogress.size} jobs: #{ex.message}")
    end

    private

    # Detect "old" jobs and requeue them because the worker they were assigned
    # to probably failed miserably.
    # NOTE Potential problem here if a specific job always make a worker
    # really fail.
    def clean_working_queues!
      return unless take_lease

      Sidekiq.logger.info "Cleaning working queues"

      @unique_queues.each do |queue|
        clean_working_queue!(queue)
      end

      @nb_fetched_jobs = 0
    end

    def take_lease
      return unless Time.now.to_f - @last_try_to_take_lease_at > @take_lease_time_interval

      @last_try_to_take_lease_at = Time.now.to_f

      Sidekiq.redis do |conn|
        redis.set('reliable-fetcher-cleanup-lock', 1, nx: true, ex: @cleanup_interval)
      end
    end

    def clean_working_queue!(queue)
      Sidekiq.redis do |conn|
        working_jobs = conn.lrange("#{queue}:#{WORKING_QUEUE}", 0, -1)
        working_jobs.each do |job|
          enqueued_at = Sidekiq.load_json(job)['enqueued_at'].to_i
          job_duration = Time.now.to_i - enqueued_at

          next if job_duration < @consider_dead_after

          Sidekiq.logger.info "Requeued a dead job from #{queue}:#{WORKING_QUEUE}"

          conn.lpush("#{queue}", job)
          conn.lrem("#{queue}:#{WORKING_QUEUE}", 1, job)
        end
      end
    end
  end
end
