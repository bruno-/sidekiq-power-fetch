Gem::Specification.new do |s|
  s.name        = 'gitlab-sidekiq-fetcher'
  s.version     = '0.4.0'
  s.authors     = ['TEA', 'GitLab']
  s.email       = 'valery@gitlab.com'
  s.license     = 'LGPL-3.0'
  s.homepage    = 'https://gitlab.com/gitlab-org/sidekiq-reliable-fetch/'
  s.summary     = 'Reliable fetch extension for Sidekiq'
  s.description = 'Redis reliable queue pattern implemented in Sidekiq'
  s.require_paths = ['lib']
  s.files = `git ls-files`.split($\)
  s.test_files  = []
  s.add_dependency 'sidekiq', '~> 5'
end
