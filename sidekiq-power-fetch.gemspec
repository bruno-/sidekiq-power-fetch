Gem::Specification.new do |s|
  s.name = "sidekiq-power-fetch"
  s.version = "0.0.1"
  s.authors = ["TEA", "GitLab", "Bruno Sutic"]
  s.email = "code@brunosutic.com"
  s.license = "LGPL-3.0"
  s.homepage = "https://gitlab.com/bruno-/sidekiq-power-fetch"
  s.summary = "Improved fetch for Sidekiq 7"
  s.require_paths = ["lib"]
  s.files = Dir["lib/**/*"]

  s.add_dependency "sidekiq", "~> 7.0"

  s.add_development_dependency "rspec", "~> 3.12"
  s.add_development_dependency "rubocop-rspec", "~> 2.23"
  s.add_development_dependency "standard", "~> 1.30"
end
