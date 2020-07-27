# frozen_string_literal: true

class ReliabilityTestWorker
  include Sidekiq::Worker

  def perform
    # To mimic long running job and to increase the probability of losing the job
    sleep 1

    Sidekiq.redis do |redis|
      redis.lpush(REDIS_FINISHED_LIST, get_sidekiq_job_id)
    end
  end

  def get_sidekiq_job_id
    jid = sidekiq_context.is_a?(Hash) ? sidekiq_context[:jid] : sidekiq_context&.first

    return unless jid

    prefix_index = jid.index('JID-')

    prefix_index ? jid[prefix_index + 4..-1] : jid
  end

  def sidekiq_context
    @sidekiq_context ||= Thread.current[:sidekiq_context]
  end
end
