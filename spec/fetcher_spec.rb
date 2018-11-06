require 'spec_helper'
require 'sidekiq/reliable_fetcher'

describe Sidekiq::ReliableFetcher do
  before do
    Sidekiq.redis = { url: REDIS_URL }
    Sidekiq.redis do |conn|
      conn.flushdb
    end
  end

  after do
    Sidekiq.redis = REDIS
  end

  shared_examples 'Sidekiq fetcher' do
    it 'retrieves the job and puts it to working queue' do
      Sidekiq.redis { |conn| conn.rpush('queue:basic', 'msg') }

      uow = fetcher.retrieve_work

      expect(working_queue_size('basic')).to eq 1
      expect(uow.queue_name).to eq 'basic'
      expect(uow.job).to eq 'msg'
      expect(Sidekiq::Queue.new('basic').size).to eq 0
    end

    it 'does not retrieve a job from foreign queue' do
      Sidekiq.redis { |conn| conn.rpush('queue:foreign', 'msg') }

      expect(fetcher.retrieve_work).to be_nil
    end

    it 'requeues job' do
      Sidekiq.redis { |conn| conn.rpush('queue:basic', 'msg') }

      uow = fetcher.retrieve_work

      uow.requeue

      expect(Sidekiq::Queue.new('basic').size).to eq 1
      expect(working_queue_size('basic')).to eq 0
    end

    it 'acknowledge job' do
      Sidekiq.redis { |conn| conn.rpush('queue:basic', 'msg') }

      uow = fetcher.retrieve_work

      uow.acknowledge

      expect(Sidekiq::Queue.new('basic').size).to eq 0
      expect(working_queue_size('basic')).to eq 0
    end

    it 'requeues jobs from dead working queue' do
      Sidekiq.redis do |conn|
        conn.rpush(foreign_working_queue_name('basic'), 'msg')
      end

      work_unit = fetcher.retrieve_work

      expect(work_unit.job).to eq 'msg'

      Sidekiq.redis do |conn|
        expect(conn.llen(foreign_working_queue_name('basic'))).to eq 0
      end
    end

    it 'does not requeue jobs from live working queue' do
      working_queue = live_foreign_working_queue_name('basic')

      Sidekiq.redis do |conn|
        conn.rpush(working_queue, 'msg')
      end

      work_unit = fetcher.retrieve_work

      expect(work_unit).to be_nil

      Sidekiq.redis do |conn|
        expect(conn.llen(working_queue)).to eq 1
      end
    end
  end

  context 'Reliable fetcher' do
    let(:fetcher) { described_class.new(queues: ['basic']) }

    include_examples 'Sidekiq fetcher'
  end

  context 'Semi-reliable fetcher' do
    let(:fetcher) { described_class.new(semi_reliable_fetch: true, queues: ['basic']) }

    include_examples 'Sidekiq fetcher'
  end

  it 'bulk requeues' do
    queue1 = Sidekiq::Queue.new('foo')
    queue2 = Sidekiq::Queue.new('bar')

    expect(queue1.size).to eq 0
    expect(queue2.size).to eq 0

    uow = described_class::UnitOfWork
    described_class.bulk_requeue([uow.new('queue:foo', 'bob'), uow.new('queue:foo', 'bar'), uow.new('queue:bar', 'widget')], {queues: []})

    expect(queue1.size).to eq 2
    expect(queue2.size).to eq 1
  end

  it 'sets heartbeat' do
    SidekiqConfigMock = Struct.new(:options)
    config = SidekiqConfigMock.new({})

    heartbeat_thread = described_class.setup_reliable_fetch!(config)

    Sidekiq.redis do |conn|
      sleep 0.2 # Give the time to heartbeat thread to make a loop
      hearbeat = conn.get(described_class.heartbeat_key(Socket.gethostname, ::Process.pid))

      expect(hearbeat).not_to be_nil
    end

    heartbeat_thread.kill
  end

  it 'runs cleanup ones for any given moment' do
    expect(described_class).to receive(:clean_working_queues!).once

    threads = []
    10.times do
      threads << Thread.new do
        described_class.new(queues: ['basic']).retrieve_work
      end
    end
    threads.map(&:join)
  end

  def working_queue_size(queue)
    Sidekiq.redis {|c| c.llen(working_queue_name(queue)) }
  end

  def working_queue_name(queue)
    "#{described_class::WORKING_QUEUE}:queue:#{queue}:#{Socket.gethostname}:#{::Process.pid}"
  end

  def foreign_working_queue_name(queue)
    "#{described_class::WORKING_QUEUE}:queue:#{queue}:#{Socket.gethostname}:#{::Process.pid + 1}"
  end

  def live_foreign_working_queue_name(queue)
    pid = ::Process.pid + 1
    hostname = Socket.gethostname

    Sidekiq.redis do |conn|
      conn.set(described_class.heartbeat_key(hostname, pid), 1)
    end

    "#{described_class::WORKING_QUEUE}:queue:#{queue}:#{hostname}:#{pid}"
  end
end
