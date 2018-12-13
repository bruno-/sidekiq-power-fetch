require 'spec_helper'
require 'fetch_shared_examples'
require 'sidekiq/base_reliable_fetch'
require 'sidekiq/reliable_fetch'
require 'sidekiq/semi_reliable_fetch'

describe Sidekiq::BaseReliableFetch do
  before { Sidekiq.redis(&:flushdb) }

  describe 'UnitOfWork' do
    let(:fetcher) { Sidekiq::ReliableFetch.new(queues: ['foo']) }

    describe '#requeue' do
      it 'requeues job' do
        Sidekiq.redis { |conn| conn.rpush('queue:foo', 'msg') }

        uow = fetcher.retrieve_work

        uow.requeue

        expect(Sidekiq::Queue.new('foo').size).to eq 1
        expect(working_queue_size('foo')).to eq 0
      end
    end

    describe '#acknowledge' do
      it 'acknowledges job' do
        Sidekiq.redis { |conn| conn.rpush('queue:foo', 'msg') }

        uow = fetcher.retrieve_work

        expect { uow.acknowledge }
          .to change { working_queue_size('foo') }.by(-1)

        expect(Sidekiq::Queue.new('foo').size).to eq 0
      end
    end
  end

  describe '.bulk_requeue' do
    it 'requeues the bulk' do
      queue1 = Sidekiq::Queue.new('foo')
      queue2 = Sidekiq::Queue.new('bar')

      expect(queue1.size).to eq 0
      expect(queue2.size).to eq 0

      uow = described_class::UnitOfWork
      jobs = [ uow.new('queue:foo', 'bob'), uow.new('queue:foo', 'bar'), uow.new('queue:bar', 'widget') ]
      described_class.bulk_requeue(jobs, queues: [])

      expect(queue1.size).to eq 2
      expect(queue2.size).to eq 1
    end
  end

  it 'sets heartbeat' do
    config = double(:sidekiq_config, options: {})

    heartbeat_thread = described_class.setup_reliable_fetch!(config)

    Sidekiq.redis do |conn|
      sleep 0.2 # Give the time to heartbeat thread to make a loop

      heartbeat_key = described_class.heartbeat_key(Socket.gethostname, ::Process.pid)
      heartbeat = conn.get(heartbeat_key)

      expect(heartbeat).not_to be_nil
    end

    heartbeat_thread.kill
  end
end
