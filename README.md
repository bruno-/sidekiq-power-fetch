sidekiq-power-fetch
===================

## Introduction

`sidekiq-power-fetch` is a Sidekiq extension that improves Sidekiq fetches from
Redis.

It's based on https://gitlab.com/gitlab-org/ruby/gems/sidekiq-reliable-fetch.

## Installation

Add the following to your `Gemfile`:

```ruby
gem "sidekiq-power-fetch"
```

## Setup

To enable reliable fetches:

```ruby
Sidekiq.configure_server do |config|
  Sidekiq::PowerFetch.setup!(config)

  # ...
end
```

## License

LGPL-3.0, see the [LICENSE file](LICENSE).
