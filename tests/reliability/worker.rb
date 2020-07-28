# frozen_string_literal: true

class ReliabilityTestWorker
  include Sidekiq::Worker

  def perform
    # To mimic long running job and to increase the probability of losing the job
    sleep 1

    Sidekiq.redis do |redis|
      redis.lpush(REDIS_FINISHED_LIST, sidekiq_job_id)
    end
  end

  def sidekiq_job_id
    Thread.current[:sidekiq_context][:jid]
  end
end
