require 'benchmark'

require_relative 'redis_server.rb'

instance = RedisServer.new

time = Benchmark.measure do
  windows_input = "/clustertmp/brennecke/jurczak/projects/genome-tiles/results/dmel-all-chromosome-r5.51.noUex.win32.bed"
  anchor_input = "/clustertmp/brennecke/jurczak/projects/genome-tiles/results/dmel-all-chromosome-r5.51.noUex.win20.bed"
  instance.perform(windows_input, anchor_input)
end

puts time
