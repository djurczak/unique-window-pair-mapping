require 'bio'
require 'redis'

require 'benchmark'

time = Benchmark.measure do
  fasta_input = "/clustertmp/brennecke/jurczak/projects/redis_test/11505.head"

  redis = Redis.new(:host => "localhost", :port => 6379, :db => 1)
  puts fasta_input

  Bio::FlatFile.auto(fasta_input) do |ff|
    ff.each_with_index do |entry, i|
      redis.set(entry.definition, entry.seq.length)
      puts i if i % 77777 == 0
    end
  end

  redis.shutdown
end

puts time
