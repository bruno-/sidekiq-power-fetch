gitlab-sidekiq-fetcher
======================

`gitlab-sidekiq-fetcher` is an extension to Sidekiq that adds support for reliable
fetches from Redis.

It's based on https://github.com/TEA-ebook/sidekiq-reliable-fetch.
At this time we only added Sidekiq 5+ support to it.

It implements in Sidekiq the reliable queue pattern using [Redis' rpoplpush
command](http://redis.io/commands/rpoplpush#pattern-reliable-queue).

## Installation

Add the following to your `Gemfile`:

```ruby
gem 'gitlab-sidekiq-fetcher', require: 'sidekiq-reliable-fetch'
```

## Configuration

Enable reliable fetches by calling this gem from your Sidekiq configuration:

```ruby
Sidekiq.configure_server do |config|
  Sidekiq::ReliableFetcher.setup_reliable_fetch!(config)

  # …
end
```

## License

LGPL-3.0, see the LICENSE file.
