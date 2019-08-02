# frozen_string_literal: true

class RetryTestWorker
  include Sidekiq::Worker

  EXPECTED_NUM_TIMES_BEEN_RUN = 2

  sidekiq_options retry: EXPECTED_NUM_TIMES_BEEN_RUN

  sidekiq_retry_in do |count, exception|
    1 # retry in one second
  end

  def perform
    sleep 1

    Sidekiq.redis do |redis|
      redis.incr('times_has_been_run')
    end

    Process.kill('KILL', Process.pid) # Job suicide, OOM killer imitation
  end
end
