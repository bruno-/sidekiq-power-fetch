require 'spec_helper'
require 'sidekiq/reliable_fetcher'

describe Sidekiq::ReliableFetcher do
  before do
    Sidekiq.redis = { :url => REDIS_URL }
    Sidekiq.redis do |conn|
      conn.flushdb
      conn.rpush('queue:basic', 'msg')
    end
  end

  after do
    Sidekiq.redis = REDIS
  end

  it 'retrieves the job and puts it to working queue' do
    q = Sidekiq::Queue.new('basic')
    fetch = described_class.new(:queues => ['basic', 'bar'])
    uow = fetch.retrieve_work

    expect(working_queue_size('basic')).to eq 1
    expect(uow).not_to be_nil
    expect(uow.queue_name).to eq 'basic'
    expect(uow.job).to eq 'msg'
    expect(q.size).to eq 0

    uow.requeue

    expect(q.size).to eq 1
    expect(working_queue_size('basic')).to eq 0
  end

  it 'bulk requeues' do
    q1 = Sidekiq::Queue.new('foo')
    q2 = Sidekiq::Queue.new('bar')

    expect(q1.size).to eq 0
    expect(q2.size).to eq 0

    uow = described_class::UnitOfWork
    described_class.bulk_requeue([uow.new('queue:foo', 'bob'), uow.new('queue:foo', 'bar'), uow.new('queue:bar', 'widget')], {:queues => []})

    expect(q1.size).to eq 2
    expect(q2.size).to eq 1
  end

  def working_queue_size(queue)
    Sidekiq.redis {|c| c.llen("queue:#{queue}:#{described_class::WORKING_QUEUE}") }
  end
end
