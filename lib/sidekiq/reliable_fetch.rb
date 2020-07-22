# frozen_string_literal: true

module Sidekiq
  class ReliableFetch < BaseReliableFetch
    # For reliable fetch we don't use Redis' blocking operations so
    # we inject a regular sleep into the loop.
    RELIABLE_FETCH_IDLE_TIMEOUT = 5 # seconds

    attr_reader :queues_iterator, :queues_size

    def initialize(options)
      super

      create_queues_iterator
    end

    private

    def retrieve_unit_of_work
      # We need to create a new Enumerator instance for Sidekiq 6.1 because the one created in the
      # class initializer has been created at the time of sidekiq initialization and would cause FiberError
      # when a thread would try to iterate (using next). Enumerator is not thread-safe.
      # We need a mechanism that allows us to have an iterator that is uniq for each thread.
      # Queue won't help here as it is cross-thread (thread-safe).
      # We skip that for Sidekiq 6.1 because before fetch API change each thread has it's own instance of the fetch class.
      create_queues_iterator if self.class.sidekiq_fetch_strategy_api_needs_instance?

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

    def create_queues_iterator
      @queues_size = queues.size
      @queues_iterator = queues.cycle
    end
  end
end
