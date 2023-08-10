require "sidekiq/power_fetch"
require "sidekiq/capsule"

RSpec.shared_context :power_fetch do
  let(:queues) { ["assigned"] }
  let(:power_fetch_lock) { nil }
  let(:power_fetch_recover) { nil }
  let(:config) { Hash.new } # Additional config options.
  let(:config_object) do
    Sidekiq.default_configuration.tap do |c|
      c[:power_fetch_lock] = power_fetch_lock
      c[:power_fetch_recover] = power_fetch_recover

      config.each do |option, value|
        c[option] = value
      end
    end
  end
  let(:capsule) do
    Sidekiq::Capsule.new("default", config_object).tap do |capsule|
      capsule.queues = queues
    end
  end
  let(:job) { Sidekiq.dump_json(class: "Bob", args: [1, 2, "foo"]) }
  let(:fetcher) { Sidekiq::PowerFetch.new(capsule) }

  before { Sidekiq.redis(&:flushdb) }
end

def working_queue_size(queue_name)
  Sidekiq.redis do |c|
    c.llen(Sidekiq::PowerFetch.working_queue_name("queue:#{queue_name}"))
  end
end
