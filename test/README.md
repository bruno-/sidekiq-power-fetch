# How to run

```
cd test
bundle exec ruby reliability_test.rb
```

You can adjust some parameters of the test in the `config.rb`


# How it works

This tool spawns configured number of Sidekiq workers and when the amount of processed jobs is about half of origin
number it will kill all the workers with `kill -9` and then it will spawn new workers again until all the jobs are processed. To track the process and counters we use Redis keys/counters.
