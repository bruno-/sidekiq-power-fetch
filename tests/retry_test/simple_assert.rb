def assert(text, actual, expected)
  if actual == expected
    puts "#{text}: #{actual} (Success)"
  else
    puts "#{text}: #{actual} (Failed). Expected: #{expected}"
    exit 1
  end
end
