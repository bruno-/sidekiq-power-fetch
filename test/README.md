# How to run

```
cd test
bundle exec ruby reliability_test.rb
```

You can adjust some parameters of the test in the `config.rb`


# How it works

This tool spawns configured number of Sidekiq workers and when the amount of processed jobs is about half of origin
number it will kill all the workers with `kill -9` and then it will spawn new workers again until all the jobs are processed. To track the process and counters we use Redis keys/counters.

# How to run tests

To run rspec:

```
bundle exec rspec
```

To run performance tests:

```
cd test
JOB_FETCHER=semi bundle exec ruby reliability_test.rb
```

JOB_FETCHER can be set to one of these values: `semi`, `reliable`, `basic`

To run both kind of tests you need to have redis server running on default HTTP port `6379`. To use other HTTP port, you can define
`REDIS_URL` environment varible with the port you need(example: `REDIS_URL="redis://localhost:9999"`).
