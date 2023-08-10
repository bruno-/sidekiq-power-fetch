RSpec.describe Sidekiq::PowerFetch::Heartbeat do
  include_context :power_fetch

  before do
    described_class.start(config_object)
    capsule.fire_event(:startup)
  end

  it "pulses" do
    Sidekiq.redis do |conn|
      key = described_class.key(Sidekiq::PowerFetch.identity)
      heartbeat = conn.get(key)

      expect(heartbeat).not_to be_nil
    end
  end
end

