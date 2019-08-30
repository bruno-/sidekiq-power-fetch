# frozen_string_literal: true

require_relative 'interrupted_set'

module Sidekiq
  class BaseReliableFetch
    DEFAULT_CLEANUP_INTERVAL = 60 * 60  # 1 hour
    HEARTBEAT_INTERVAL       = 20       # seconds
    HEARTBEAT_LIFESPAN       = 60       # seconds
    HEARTBEAT_RETRY_DELAY    = 1        # seconds
    WORKING_QUEUE_PREFIX     = 'working'

    # Defines how often we try to take a lease to not flood our
    # Redis server with SET requests
    DEFAULT_LEASE_INTERVAL = 2 * 60 # seconds
    LEASE_KEY              = 'reliable-fetcher-cleanup-lock'

    # Defines the COUNT parameter that will be passed to Redis SCAN command
    SCAN_COUNT = 1000

    # How much time a job can be interrupted
    DEFAULT_MAX_RETRIES_AFTER_INTERRUPTION = 3

    UnitOfWork = Struct.new(:queue, :job) do
      def acknowledge
        Sidekiq.redis { |conn| conn.lrem(Sidekiq::BaseReliableFetch.working_queue_name(queue), 1, job) }
      end

      def queue_name
        queue.sub(/.*queue:/, '')
      end

      def requeue
        Sidekiq.redis do |conn|
          conn.multi do |multi|
            multi.lpush(queue, job)
            multi.lrem(Sidekiq::BaseReliableFetch.working_queue_name(queue), 1, job)
          end
        end
      end
    end

    def self.setup_reliable_fetch!(config)
      config.options[:fetch] = if config.options[:semi_reliable_fetch]
                                 Sidekiq::SemiReliableFetch
                               else
                                 Sidekiq::ReliableFetch
                               end

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

            sleep HEARTBEAT_RETRY_DELAY
          end
        end
      end
    end

    def self.pid
      @pid ||= ::Process.pid
    end

    def self.hostname
      @hostname ||= Socket.gethostname
    end

    def self.heartbeat
      Sidekiq.redis do |conn|
        conn.set(heartbeat_key(hostname, pid), 1, ex: HEARTBEAT_LIFESPAN)
      end

      Sidekiq.logger.debug("Heartbeat for hostname: #{hostname} and pid: #{pid}")
    end

    def self.bulk_requeue(inprogress, _options)
      return if inprogress.empty?

      Sidekiq.redis do |conn|
        inprogress.each do |unit_of_work|
          conn.multi do |multi|
            preprocess_interrupted_job(unit_of_work.job, unit_of_work.queue, multi)

            multi.lrem(working_queue_name(unit_of_work.queue), 1, unit_of_work.job)
          end
        end
      end
    rescue => e
      Sidekiq.logger.warn("Failed to requeue #{inprogress.size} jobs: #{e.message}")
    end

    def self.clean_working_queue!(working_queue)
      original_queue = working_queue.gsub(/#{WORKING_QUEUE_PREFIX}:|:[^:]*:[0-9]*\z/, '')

      Sidekiq.redis do |conn|
        while job = conn.rpop(working_queue)
          preprocess_interrupted_job(job, original_queue)
        end
      end
    end

    def self.preprocess_interrupted_job(job, queue, conn = nil)
      msg = Sidekiq.load_json(job)
      msg['interrupted_count'] = msg['interrupted_count'].to_i + 1

      if interruption_exhausted?(msg)
        send_to_quarantine(msg, conn)
      else
        requeue_job(queue, msg, conn)
      end
    end

    # Detect "old" jobs and requeue them because the worker they were assigned
    # to probably failed miserably.
    def self.clean_working_queues!
      Sidekiq.logger.info('Cleaning working queues')

      Sidekiq.redis do |conn|
        conn.scan_each(match: "#{WORKING_QUEUE_PREFIX}:queue:*", count: SCAN_COUNT) do |key|
          # Example: "working:name_of_the_job:queue:{hostname}:{PID}"
          hostname, pid = key.scan(/:([^:]*):([0-9]*)\z/).flatten

          continue if hostname.nil? || pid.nil?

          clean_working_queue!(key) if worker_dead?(hostname, pid, conn)
        end
      end
    end

    def self.worker_dead?(hostname, pid, conn)
      !conn.get(heartbeat_key(hostname, pid))
    end

    def self.heartbeat_key(hostname, pid)
      "reliable-fetcher-heartbeat-#{hostname}-#{pid}"
    end

    def self.working_queue_name(queue)
      "#{WORKING_QUEUE_PREFIX}:#{queue}:#{hostname}:#{pid}"
    end

    def self.interruption_exhausted?(msg)
      return false if max_retries_after_interruption(msg['class']) < 0

      msg['interrupted_count'].to_i >= max_retries_after_interruption(msg['class'])
    end

    def self.max_retries_after_interruption(worker_class)
      max_retries_after_interruption = nil

      max_retries_after_interruption ||= begin
        Object.const_get(worker_class).sidekiq_options[:max_retries_after_interruption]
      rescue NameError
      end

      max_retries_after_interruption ||= Sidekiq.options[:max_retries_after_interruption]
      max_retries_after_interruption ||= DEFAULT_MAX_RETRIES_AFTER_INTERRUPTION
      max_retries_after_interruption
    end

    def self.send_to_quarantine(msg, multi_connection = nil)
      Sidekiq.logger.warn(
        class: msg['class'],
        jid: msg['jid'],
        message: %(Reliable Fetcher: adding dead #{msg['class']} job #{msg['jid']} to interrupted queue)
      )

      job = Sidekiq.dump_json(msg)
      Sidekiq::InterruptedSet.new.put(job, connection: multi_connection)
    end

    # If you want this method to be run is a scope of multi connection
    # you need to pass it
    def self.requeue_job(queue, msg, conn)
      with_connection(conn) do |conn|
        conn.lpush(queue, Sidekiq.dump_json(msg))
      end

      Sidekiq.logger.info(
        message: "Pushed job #{msg['jid']} back to queue #{queue}",
        jid: msg['jid'],
        queue: queue
      )
    end

    # Yield block with an existing connection or creates another one
    def self.with_connection(conn, &block)
      return yield(conn) if conn

      Sidekiq.redis { |conn| yield(conn) }
    end

    attr_reader :cleanup_interval, :last_try_to_take_lease_at, :lease_interval,
                :queues, :use_semi_reliable_fetch,
                :strictly_ordered_queues

    def initialize(options)
      @cleanup_interval = options.fetch(:cleanup_interval, DEFAULT_CLEANUP_INTERVAL)
      @lease_interval = options.fetch(:lease_interval, DEFAULT_LEASE_INTERVAL)
      @last_try_to_take_lease_at = 0
      @strictly_ordered_queues = !!options[:strict]
      @queues = options[:queues].map { |q| "queue:#{q}" }
    end

    def retrieve_work
      self.class.clean_working_queues! if take_lease

      retrieve_unit_of_work
    end

    def retrieve_unit_of_work
      raise NotImplementedError,
        "#{self.class} does not implement #{__method__}"
    end

    private

    def take_lease
      return unless allowed_to_take_a_lease?

      @last_try_to_take_lease_at = Time.now.to_f

      Sidekiq.redis do |conn|
        conn.set(LEASE_KEY, 1, nx: true, ex: cleanup_interval)
      end
    end

    def allowed_to_take_a_lease?
      Time.now.to_f - last_try_to_take_lease_at > lease_interval
    end
  end
end
