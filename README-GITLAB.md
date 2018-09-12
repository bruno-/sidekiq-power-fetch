gitlab-sidekiq-fetcher
======================

# How to publish a new release?

1. Dev-commit cycle
2. Update the version in the gemspec file, commit and tag
3. Build the gem: `gem build gitlab-sidekiq-fetcher.gemspec`
4. Upload the gem: `gem push gitlab-sidekiq-fetcher-X.X.X.gem`
