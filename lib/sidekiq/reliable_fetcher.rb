# frozen_string_literal: true

module Sidekiq
  class ReliableFetcher
    WORKING_QUEUE            = 'working'.freeze
    DEFAULT_CLEANUP_INTERVAL = 60 * 60 # 1 hour

    # DEFAULT_LEASE_TIME_INTERVAL defines how often we try to take a lease
    # to not flood our Redis server with SET requests
    DEFAULT_LEASE_TIME_INTERVAL = 2 * 60 # seconds
    IDLE_TIMEOUT                = 5      # seconds
    HEARTBEAT_INTERVAL          = 30     # seconds
    LEASE_KEY                   = 'reliable-fetcher-cleanup-lock'

    # We want the fetch operation to timeout every few seconds so the thread
    # can check if the process is shutting down. This constant is only used for semi-reliable fetch
    SEMI_RELIABLE_FETCH_TIMEOUT = 2 # seconds

    # Defines the COUNT parameter that will be passed to Redis SCAN command
    SCAN_COUNT = 1000

    UnitOfWork = Struct.new(:queue, :job) do
      def acknowledge
        Sidekiq.redis { |conn| conn.lrem(Sidekiq::ReliableFetcher.working_queue_name(queue), 1, job) }
      end

      def queue_name
        queue.sub(/.*queue:/, '')
      end

      def requeue
        Sidekiq.redis do |conn|
          conn.pipelined do
            conn.lpush(queue, job)
            conn.lrem(Sidekiq::ReliableFetcher.working_queue_name(queue), 1, job)
          end
        end
      end
    end

    def self.setup_reliable_fetch!(config)
      config.options[:fetch] = Sidekiq::ReliableFetcher

      Sidekiq.logger.info('GitLab reliable fetch activated!')

      start_heartbeat_thread
    end

    def self.start_heartbeat_thread
      Thread.new do
        loop do
          begin
            heartbeat

            sleep HEARTBEAT_INTERVAL
          rescue => e
            Sidekiq.logger.error("Heartbeat thread error: #{e.message}")
          end
        end
      end
    end

    def self.pid
      @@pid ||= ::Process.pid
    end

    def self.hostname
      @@hostname ||= Socket.gethostname
    end

    def self.heartbeat
      Sidekiq.redis do |conn|
        conn.set(heartbeat_key(hostname, pid), 1, ex: HEARTBEAT_INTERVAL * 2)
      end

      Sidekiq.logger.debug("Heartbeat for hostname: #{hostname} and pid: #{pid}")
    end

    def initialize(options)
      @queues = options[:queues].map { |q| "queue:#{q}" }.shuffle

      @queues_iterator = @queues.cycle
      @queues_size = @queues.size

      @cleanup_interval = options.fetch(:cleanup_interval, DEFAULT_CLEANUP_INTERVAL)
      @lease_time_interval = options.fetch(:lease_time_interval, DEFAULT_LEASE_TIME_INTERVAL)
      @last_try_to_take_lease_at = 0

      @semi_reliable_fetch = options[:semi_reliable_fetch]
    end

    def retrieve_work
      clean_working_queues! if take_lease

      if @semi_reliable_fetch
        semi_reliable_fetch
      else
        reliable_fetch
      end
    end

    def self.bulk_requeue(inprogress, _options)
      return if inprogress.empty?

      Sidekiq.logger.debug('Re-queueing terminated jobs')

      Sidekiq.redis do |conn|
        conn.pipelined do
          inprogress.each do |unit_of_work|
            conn.lpush(unit_of_work.queue, unit_of_work.job)
            conn.lrem(working_queue_name(unit_of_work.queue), 1, unit_of_work.job)
          end
        end
      end

      Sidekiq.logger.info("Pushed #{inprogress.size} jobs back to Redis")
    rescue => e
      Sidekiq.logger.warn("Failed to requeue #{inprogress.size} jobs: #{e.message}")
    end

    def self.heartbeat_key(hostname, pid)
      "reliable-fetcher-heartbeat-#{hostname}-#{pid}"
    end

    def self.working_queue_name(queue)
      "#{WORKING_QUEUE}:#{queue}:#{hostname}:#{pid}"
    end

    private

    def semi_reliable_fetch
      work = Sidekiq.redis { |conn| conn.brpop(*queues_cmd) }
      return unless work

      unit_of_work = UnitOfWork.new(*work)

      Sidekiq.redis do |conn|
        conn.lpush(self.class.working_queue_name(unit_of_work.queue), unit_of_work.job)
      end

      unit_of_work
    end

    def reliable_fetch
      @queues_size.times do
        queue = @queues_iterator.next

        work = Sidekiq.redis do |conn|
          conn.rpoplpush(queue, self.class.working_queue_name(queue))
        end

        return UnitOfWork.new(queue, work) if work
      end

      # We didn't find a job in any of the configured queues. Let's sleep a bit
      # to avoid uselessly burning too much CPU
      sleep(IDLE_TIMEOUT)

      nil
    end

    def queues_cmd
      queues = @queues.uniq
      queues << SEMI_RELIABLE_FETCH_TIMEOUT
      queues
    end

    def clean_working_queue!(working_queue)
      original_queue = working_queue.gsub(/#{WORKING_QUEUE}:|:[^:]*:[0-9]*\z/, '')

      Sidekiq.redis do |conn|
        count = 0

        while conn.rpoplpush(working_queue, original_queue) do
          count += 1
        end

        Sidekiq.logger.info("Requeued #{count} dead jobs to #{original_queue}")
      end
    end

    # Detect "old" jobs and requeue them because the worker they were assigned
    # to probably failed miserably.
    def clean_working_queues!
      Sidekiq.logger.info("Cleaning working queues")

      Sidekiq.redis do |conn|
        conn.scan_each(match: "#{WORKING_QUEUE}:queue:*", count: SCAN_COUNT) do |key|
          # Example: "working:name_of_the_job:queue:{hostname}:{PID}"
          hostname, pid = key.scan(/:([^:]*):([0-9]*)\z/).flatten

          continue if hostname.nil? || pid.nil?

          clean_working_queue!(key) if worker_dead?(hostname, pid)
        end
      end
    end

    def worker_dead?(hostname, pid)
      Sidekiq.redis do |conn|
        !conn.get(self.class.heartbeat_key(hostname, pid))
      end
    end

    def take_lease
      return unless allowed_to_take_a_lease?

      @last_try_to_take_lease_at = Time.now.to_f

      Sidekiq.redis do |conn|
        conn.set(LEASE_KEY, 1, nx: true, ex: @cleanup_interval)
      end
    end

    def allowed_to_take_a_lease?
      Time.now.to_f - @last_try_to_take_lease_at > @lease_time_interval
    end
  end
end
