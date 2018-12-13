shared_examples 'a Sidekiq fetcher' do
  let(:queues) { ['assigned'] }

  before { Sidekiq.redis(&:flushdb) }

  describe '#retrieve_work' do
    let(:fetcher) { described_class.new(queues: ['assigned']) }

    it 'retrieves the job and puts it to working queue' do
      Sidekiq.redis { |conn| conn.rpush('queue:assigned', 'msg') }

      uow = fetcher.retrieve_work

      expect(working_queue_size('assigned')).to eq 1
      expect(uow.queue_name).to eq 'assigned'
      expect(uow.job).to eq 'msg'
      expect(Sidekiq::Queue.new('assigned').size).to eq 0
    end

    it 'does not retrieve a job from foreign queue' do
      Sidekiq.redis { |conn| conn.rpush('queue:not_assigned', 'msg') }

      expect(fetcher.retrieve_work).to be_nil
    end

    it 'requeues jobs from dead working queue' do
      Sidekiq.redis do |conn|
        conn.rpush(other_process_working_queue_name('assigned'), 'msg')
      end

      uow = fetcher.retrieve_work

      expect(uow.job).to eq 'msg'

      Sidekiq.redis do |conn|
        expect(conn.llen(other_process_working_queue_name('assigned'))).to eq 0
      end
    end

    it 'does not requeue jobs from live working queue' do
      working_queue = live_other_process_working_queue_name('assigned')

      Sidekiq.redis do |conn|
        conn.rpush(working_queue, 'msg')
      end

      uow = fetcher.retrieve_work

      expect(uow).to be_nil

      Sidekiq.redis do |conn|
        expect(conn.llen(working_queue)).to eq 1
      end
    end

    it 'does not clean up orphaned jobs more than once per cleanup interval' do
      Sidekiq.redis = Sidekiq::RedisConnection.create(url: REDIS_URL, size: 10)

      expect_any_instance_of(described_class)
        .to receive(:clean_working_queues!).once

      threads = 10.times.map do
        Thread.new do
          described_class.new(queues: ['assigned']).retrieve_work
        end
      end

      threads.map(&:join)
    end

    it 'retrieves by order when strictly order is enabled' do
      fetcher = described_class.new(strict: true, queues: ['first', 'second'])

      Sidekiq.redis do |conn|
        conn.rpush('queue:first', ['msg3', 'msg2', 'msg1'])
        conn.rpush('queue:second', 'msg4')
      end

      jobs = (1..4).map { fetcher.retrieve_work.job }

      expect(jobs).to eq ['msg1', 'msg2', 'msg3', 'msg4']
    end

    it 'does not starve any queue when queues are not strictly ordered' do
      fetcher = described_class.new(queues: ['first', 'second'])

      Sidekiq.redis do |conn|
        conn.rpush('queue:first', (1..200).map { |i| "msg#{i}" })
        conn.rpush('queue:second', 'this_job_should_not_stuck')
      end

      jobs = (1..100).map { fetcher.retrieve_work.job }

      expect(jobs).to include 'this_job_should_not_stuck'
    end
  end
end

def working_queue_size(queue_name)
  Sidekiq.redis do |c|
    c.llen(Sidekiq::BaseReliableFetch.working_queue_name("queue:#{queue_name}"))
  end
end

def other_process_working_queue_name(queue)
  "#{Sidekiq::BaseReliableFetch::WORKING_QUEUE_PREFIX}:queue:#{queue}:#{Socket.gethostname}:#{::Process.pid + 1}"
end

def live_other_process_working_queue_name(queue)
  pid = ::Process.pid + 1
  hostname = Socket.gethostname

  Sidekiq.redis do |conn|
    conn.set(Sidekiq::BaseReliableFetch.heartbeat_key(hostname, pid), 1)
  end

  "#{Sidekiq::BaseReliableFetch::WORKING_QUEUE_PREFIX}:queue:#{queue}:#{hostname}:#{pid}"
end
