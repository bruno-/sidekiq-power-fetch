require 'spec_helper'
require 'fetch_shared_examples'
require 'sidekiq/semi_reliable_fetch'

describe Sidekiq::SemiReliableFetch do
  include_examples 'a Sidekiq fetcher'
end
