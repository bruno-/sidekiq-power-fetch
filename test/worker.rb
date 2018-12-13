# frozen_string_literal: true

class TestWorker
  include Sidekiq::Worker

  def perform
    # To mimic long running job and to increase the probability of losing the job
    sleep 1

    Sidekiq.redis do |redis|
      redis.lpush(REDIS_FINISHED_LIST, get_sidekiq_job_id)
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
