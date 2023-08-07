module Sidekiq
  class PowerFetch
    class UnitOfWork
      def initialize(queue, job)
        @queue = queue
        @job = job
      end

      def acknowledge
        Sidekiq.redis do |conn|
          conn.lrem(PowerFetch.working_queue_name(@queue), 1, @job)
        end
      end

      def queue_name
        @queue.sub(/.*queue:/, "")
      end

      def requeue
        Sidekiq.redis do |conn|
          conn.multi do |multi|
            multi.lpush(@queue, @job)
            multi.lrem(PowerFetch.working_queue_name(@queue), 1, @job)
          end
        end
      end
    end
  end
end
