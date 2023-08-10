RSpec.describe Sidekiq::PowerFetch do
  include_context :power_fetch

  describe "#retrieve_work" do
    context "multiple #retrieve_work" do
      let(:recover) { fetcher.instance_variable_get(:@recover) }
      let(:config) do
        {redis: Sidekiq::RedisConnection.create(url: REDIS_URL, size: 10)}
      end

      it "cleans orphaned jobs once per cleanup interval" do
        expect(recover).to receive(:call).once

        threads = 10.times.map do
          Thread.new do
            fetcher.retrieve_work
          end
        end

        threads.map(&:join)
      end
    end

    context "when strict order is enabled" do
      let(:queues) { ["first", "second"] }

      it "retrieves by order" do
        Sidekiq.redis do |conn|
          conn.rpush("queue:first", ["msg3", "msg2", "msg1"])
          conn.rpush("queue:second", "msg4")
        end

        jobs = 4.times.map { fetcher.retrieve_work.job }
        expect(jobs).to eq ["msg1", "msg2", "msg3", "msg4"]
      end
    end

    context "when strict order is disabled" do
      let(:queues) { [["first", 2], ["second", 1]] }

      it "does not starve any queue" do
        Sidekiq.redis do |conn|
          conn.rpush("queue:first", (1..200).map { |i| "msg#{i}" })
          conn.rpush("queue:second", "this_job_should_not_get_stuck")
        end

        jobs = 100.times.map { fetcher.retrieve_work.job }

        expect(jobs).to include "this_job_should_not_get_stuck"
      end
    end

    shared_examples :queue do |queue|
      let(:queues) { [queue] }

      it "retrieves the job and puts it to working queue" do
        Sidekiq.redis { |conn| conn.rpush("queue:#{queue}", job) }

        uow = fetcher.retrieve_work

        expect(working_queue_size(queue)).to eq 1
        expect(uow.queue_name).to eq queue
        expect(uow.job).to eq job
        expect(Sidekiq::Queue.new(queue).size).to eq 0
      end

      it "does not retrieve a job from foreign queue" do
        Sidekiq.redis { |conn| conn.rpush("queue:#{queue}:not", job) }
        expect(fetcher.retrieve_work).to be_nil

        Sidekiq.redis { |conn| conn.rpush("queue:not_#{queue}", job) }
        expect(fetcher.retrieve_work).to be_nil

        Sidekiq.redis { |conn| conn.rpush("queue:random_name", job) }
        expect(fetcher.retrieve_work).to be_nil
      end

      it "ignores working queue keys in unknown formats" do
        # Add a spurious non-numeric char segment at the end; this simulates any other
        # incorrect form in general
        malformed_key = "#{other_process_working_queue_name(queue)}:X"
        Sidekiq.redis do |conn|
          conn.rpush(malformed_key, job)
        end

        _ = fetcher.retrieve_work

        Sidekiq.redis do |conn|
          expect(conn.llen(malformed_key)).to eq 1
        end
      end

      it "requeues jobs from dead working queue with incremented interrupted_count" do
        Sidekiq.redis do |conn|
          conn.rpush(other_process_working_queue_name(queue), job)
        end

        expected_job = Sidekiq.load_json(job)
        expected_job["interrupted_count"] = 1
        expected_job = Sidekiq.dump_json(expected_job)

        uow = fetcher.retrieve_work

        expect(uow).to_not be_nil
        expect(uow.job).to eq expected_job

        Sidekiq.redis do |conn|
          expect(conn.llen(other_process_working_queue_name(queue))).to eq 0
        end
      end

      it "does not requeue jobs from live working queue" do
        working_queue = live_other_process_working_queue_name(queue)

        Sidekiq.redis do |conn|
          conn.rpush(working_queue, job)
        end

        uow = fetcher.retrieve_work
        expect(uow).to be_nil

        Sidekiq.redis do |conn|
          expect(conn.llen(working_queue)).to eq 1
        end
      end
    end

    context "with various queues" do
      %w[assigned namespace:assigned namespace:deeper:assigned].each do |queue|
        it_behaves_like :queue, queue
      end
    end

    context "with short recover interval" do
      let(:short_interval) { 1 }
      let(:power_fetch_lock) { short_interval }
      let(:power_fetch_recover) { short_interval }

      it "requeues when there is no heartbeat" do
        Sidekiq.redis { |conn| conn.rpush("queue:assigned", job) }
        # Use of retrieve_work twice with a sleep ensures we have exercised the
        # `identity` method to create the working queue key name and that it
        # matches the patterns used in the cleanup
        _ = fetcher.retrieve_work
        sleep(short_interval + 1)
        uow = fetcher.retrieve_work

        # Will only receive a UnitOfWork if the job was detected as failed and requeued
        expect(uow).to_not be_nil
      end
    end
  end

  describe "#bulk_requeue" do
    let(:queues) { %w[foo bar] }
    let!(:queue1) { Sidekiq::Queue.new("foo") }
    let!(:queue2) { Sidekiq::Queue.new("bar") }
    let(:interrupted_job) do
      Sidekiq.dump_json(class: "Bob", args: [1, 2, "foo"], interrupted_count: 3)
    end

    context "with regular jobs" do
      let(:jobs) do
        [
          unit_of_work("queue:foo", job),
          unit_of_work("queue:foo", job),
          unit_of_work("queue:bar", job)
        ]
      end

      it "requeues jobs" do
        fetcher.bulk_requeue(jobs)

        expect(queue1.size).to eq 2
        expect(queue2.size).to eq 1
      end
    end
  end
end

def other_process_working_queue_name(queue)
  "#{Sidekiq::PowerFetch::WORKING_QUEUE_PREFIX}:queue:#{queue}:#{identity}"
end

def identity
  pid = Process.pid + 1
  hostname = Socket.gethostname
  nonce = SecureRandom.hex(6)

  "#{hostname}:#{pid}:#{nonce}"
end

def live_other_process_working_queue_name(queue)
  id = identity
  Sidekiq.redis do |conn|
    key = Sidekiq::PowerFetch::Heartbeat.key(id)
    conn.set(key, 1)
  end

  "#{Sidekiq::PowerFetch::WORKING_QUEUE_PREFIX}:queue:#{queue}:#{id}"
end

def unit_of_work(...)
  Sidekiq::PowerFetch::UnitOfWork.new(...)
end
