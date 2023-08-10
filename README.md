sidekiq-power-fetch
===================

## Introduction

`sidekiq-power-fetch` is a Sidekiq extension that improves Sidekiq fetches from
Redis. It supports Sidekiq 7+ and Redis 6.2.0+.

It's based on Gitlab's reliable
[gitlab-sidekiq-fetcher](https://rubygems.org/gems/gitlab-sidekiq-fetcher),
which unfortunately doesn't support Sidekiq 7.

## Installation

Add the following to your `Gemfile`:

```ruby
gem "sidekiq-power-fetch"
```

Additional configuration is not necessary - power fetch just works.

## Why not just buy Sidekiq PRO?

By all means buy Sidekiq PRO, if your project can afford it.

However, if you need a reliable solution for a hobby project, or your
commercial project doesn't have enough revenue (yet), you can use this gem.

## License

LGPL-3.0, see the [LICENSE file](LICENSE).
