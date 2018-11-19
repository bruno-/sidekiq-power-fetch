class TestWorker
  include Sidekiq::Worker

  sidekiq_options retry: false

  def perform
    sleep 2

    Sidekiq.redis do |redis|
      # As we disabled retries, this counter is a number of unique jobs + possible duplicates
      redis.incr('reliable-fetcher-counter')

      if redis.get("reliable-fetcher-jid-#{get_sidekiq_job_id}")
        # As we disabled retries, this counter is a number of duplicates
        redis.incr("reliable-fetcher-duplicate-counter")
      else
        redis.set("reliable-fetcher-jid-#{get_sidekiq_job_id}", 1)
      end
    end
  end

  def get_sidekiq_job_id
    context_data = Thread.current[:sidekiq_context]&.first

    return unless context_data

    index = context_data.index('JID-')

    return unless index

    context_data[index + 4..-1]
  end
end
