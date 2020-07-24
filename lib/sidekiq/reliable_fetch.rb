# frozen_string_literal: true

module Sidekiq
  class ReliableFetch < BaseReliableFetch
    # For reliable fetch we don't use Redis' blocking operations so
    # we inject a regular sleep into the loop.
    RELIABLE_FETCH_IDLE_TIMEOUT = 5 # seconds

    attr_reader :queues_size

    def initialize(options)
      super

      @queues_size = queues.size
    end

    private

    def retrieve_unit_of_work
      queues_iterator.rewind if strictly_ordered_queues

      queues_size.times do
        queue = queues_iterator.next

        work = Sidekiq.redis do |conn|
          conn.rpoplpush(queue, self.class.working_queue_name(queue))
        end

        return UnitOfWork.new(queue, work) if work
      end

      # We didn't find a job in any of the configured queues. Let's sleep a bit
      # to avoid uselessly burning too much CPU
      sleep(RELIABLE_FETCH_IDLE_TIMEOUT)

      nil
    end

    private

    def queues_iterator
      # We need to keep the queues_iterator thread-local for
      # Sidekiq >= 6.1 because Enumerator is not thread-safe and we what each
      # Thread to iterate through queues in isolation
      # (not sharing the iterator between threads).
      # object_id is to restrict the iterator to the instance of this class
      # in case of multiple instances within one Thread.
      Thread.current["sidekiq_reliable_fetch_queues_iterator_#{object_id}"] ||= queues.cycle
    end
  end
end
