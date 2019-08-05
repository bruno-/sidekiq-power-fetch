# frozen_string_literal: true

require 'sidekiq'
require_relative 'config'
require_relative '../support/utils'

NUM_WORKERS = 2 # one worker will be killed and one spare worker t verify that job is not picked up

Sidekiq.redis(&:flushdb)

pids = spawn_workers(NUM_WORKERS)

jid = NoRetryTestWorker.perform_async

sleep 300

Sidekiq.redis do |redis|
  times_has_been_run = redis.get('times_has_been_run').to_i
  assert 'The job has been run', times_has_been_run, 1
end

assert 'Found dead jobs', Sidekiq::DeadSet.new.size, 1

stop_workers(pids)
