RSpec.describe Sidekiq::PowerFetch::UnitOfWork do
  include_context :power_fetch
  let(:queues) { ["foo"] }

  describe "#requeue" do
    it "requeues job" do
      Sidekiq.redis { |conn| conn.rpush("queue:foo", job) }

      uow = fetcher.retrieve_work
      uow.requeue

      expect(Sidekiq::Queue.new("foo").size).to eq 1
      expect(working_queue_size("foo")).to eq 0
    end
  end

  describe "#acknowledge" do
    it "acknowledges job" do
      Sidekiq.redis { |conn| conn.rpush("queue:foo", job) }

      uow = fetcher.retrieve_work

      expect {
        uow.acknowledge
      }.to change { working_queue_size("foo") }.by(-1)

      expect(Sidekiq::Queue.new("foo").size).to eq 0
    end
  end
end
