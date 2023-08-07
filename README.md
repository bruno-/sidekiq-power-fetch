sidekiq-power-fetch
===================

## Introduction

`sidekiq-power-fetch` is a Sidekiq extension that improves Sidekiq fetches from Redis.

It's based on https://gitlab.com/gitlab-org/ruby/gems/sidekiq-reliable-fetch.

### Interruption handling

Sidekiq expects any job to report succcess or to fail. In the last case,
Sidekiq puts `retry_count` counter into the job and keeps to re-run the job
until the counter reched the maximum allowed value. When the job has not been
given a chance to finish its work(to report success or fail), for example, when
it was killed forcibly or when the job was requeued, after receiving TERM
signal, the standard retry mechanisme does not get into the game and the job
will be retried indefinatelly. This is why Reliable fetcher maintains a special
counter `interrupted_count` which is used to limit the amount of such retries.
In both cases, Reliable Fetcher increments counter `interrupted_count` and
rejects the job from running again when the counter exceeds
`max_retries_after_interruption` times (default: 3 times). Such a job will be
put to `interrupted` queue. This queue mostly behaves as Sidekiq Dead queue so
it only stores a limited amount of jobs for a limited term. Same as for Dead
queue, all the limits are configurable via `interrupted_max_jobs` (default:
10_000) and `interrupted_timeout_in_seconds` (default: 3 months) Sidekiq option
keys.

You can also disable special handling of interrupted jobs by setting
`max_retries_after_interruption` into `-1`. In this case, interrupted jobs will
be run without any limits from Reliable Fetcher and they won't be put into
Interrupted queue.


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
