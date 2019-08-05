# frozen_string_literal: true

class NoRetryTestWorker
  include Sidekiq::Worker

  sidekiq_options retry: false

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
