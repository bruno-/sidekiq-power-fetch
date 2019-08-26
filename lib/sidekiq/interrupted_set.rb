require 'sidekiq/api'

module Sidekiq
  class InterruptedSet < ::Sidekiq::JobSet
    def initialize
      super "interrupted"
    end

    def put(message, opts = {})
      now = Time.now.to_f
      Sidekiq.redis do |conn|
        conn.multi do
          conn.zadd(name, now.to_s, message)
          conn.zremrangebyscore(name, "-inf", now - self.class.timeout)
          conn.zremrangebyrank(name, 0, - self.class.max_jobs)
        end
      end

      true
    end

    def retry_all
      each(&:retry) while size > 0
    end

    def self.max_jobs
      Sidekiq.options[:interrupted_max_jobs] || 10_000
    end

    def self.timeout
      Sidekiq.options[:interrupted_timeout_in_seconds] || 90 * 24 * 60 * 60 # 3 months
    end
  end
end
