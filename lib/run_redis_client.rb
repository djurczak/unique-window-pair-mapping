require_relative 'sync_to_redis.rb'

require 'shell_command'
require 'redis'

args = Hash.new

args["redis_host"] = "localhost"
args["redis_port"] = 13337
args["path_to_file"] = "/path/to/file"
args["mode"] = "write"

# check if the number of args is correct
if ARGV.size == 1 and ARGV[0] == '-h'
  raise "sge_walking_plot_comparison help"
# all args except step_size and read_limit need to be specified
elsif ARGV.size < (args.keys.size)
  puts args.keys.size.to_s + " - " + ARGV.size.to_s
  raise "Not all necessary arguments specified, use '-h' for help"
end

# work through all args
ARGV.each do |arg|
  name = arg.split('=')[0]
  val = arg.split('=')[1]
  if args.has_key?(name)
    args[name]= val
  else
    raise "Unknown argument <#{name}> script stopped, use '-h' for help"
  end
end

redis_host = args["redis_host"]
redis_port = args["redis_port"]
path_to_file = args["path_to_file"]
mode = args["mode"]

instance = SyncToRedis.new

## open up connection to given Redis instance
conn = Redis.new(:host => redis_host, :port => redis_port, :db => 1)
conn.ping

## run bed file through GNU uniq to get the fasta definition_line + frequencies
path_to_uniq_file = instance.convert_to_frequency_files(path_to_file)

File.open(path_to_uniq_file, 'r') do |fIn|
  if mode == "write"
    instance.push_frequencies_to_redis(conn, fIn.lines)
  else
    instance.remove_frequences_from_redis(conn, fIn.lines)
  end
end
