gitlab-sidekiq-fetcher
======================

`gitlab-sidekiq-fetcher` is an extension to Sidekiq that adds support for reliable
fetches from Redis.

It's based on https://github.com/TEA-ebook/sidekiq-reliable-fetch.

There are two strategies implemented: [Reliable fetch](http://redis.io/commands/rpoplpush#pattern-reliable-queue) using `rpoplpush` command and
semi-reliable fetch that uses regular `brpop` and `lpush` to pick the job and put it to working queue. The main benefit of "Reliable" strategy is that `rpoplpush` is atomic, eliminating a race condition in which jobs can be lost.
However, it comes at a cost because `rpoplpush` can't watch multiple lists at the same time so we need to iterate over the entire queue list which significantly increases pressure on Redis when there are more than a few queues. The "semi-reliable" strategy is much more reliable than the default Sidekiq fetcher, though. Compared to the reliable fetch strategy, it does not increase pressure on Redis significantly.


## Installation

Add the following to your `Gemfile`:

```ruby
gem 'gitlab-sidekiq-fetcher', require: 'sidekiq-reliable-fetch'
```

## Configuration

Enable reliable fetches by calling this gem from your Sidekiq configuration:

```ruby
Sidekiq.configure_server do |config|
  Sidekiq::ReliableFetch.setup_reliable_fetch!(config)

  # …
end
```

There is an additional parameter `config.options[:semi_reliable_fetch]` you can use to switch between two strategies:

```ruby
Sidekiq.configure_server do |config|
  config.options[:semi_reliable_fetch] = true # Default value is false

  Sidekiq::ReliableFetch.setup_reliable_fetch!(config)
end
```

## License

LGPL-3.0, see the LICENSE file.
