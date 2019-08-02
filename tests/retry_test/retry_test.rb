# frozen_string_literal: true

require 'sidekiq'
require 'sidekiq/util'
require 'sidekiq/cli'
require_relative 'config'
require_relative 'simple_assert'

NUM_WORKERS = RetryTestWorker::EXPECTED_NUM_TIMES_BEEN_RUN + 1

Sidekiq.redis(&:flushdb)

def spawn_workers
  pids = []

  NUM_WORKERS.times do
    pids << spawn('sidekiq -r ./config.rb')
  end

  pids
end

pids = spawn_workers

jid = RetryTestWorker.perform_async

sleep 300

Sidekiq.redis do |redis|
  times_has_been_run = redis.get('times_has_been_run').to_i
  assert "The job has been run", times_has_been_run, 2
end

assert "Found dead jobs", Sidekiq::DeadSet.new.size, 1

# Stop Sidekiq workers
pids.each do |pid|
  Process.kill('KILL', pid)
  Process.wait pid
end
