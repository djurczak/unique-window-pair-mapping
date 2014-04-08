require 'redis'
require 'thread/pool'
require 'childprocess'

require_relative 'sge_command.rb'
require_relative 'library_splitter.rb'

class RedisServer
  attr_reader :process, :redis_port

  def perform(path_to_windows, path_to_anchors)
    ## spin up new redis instance
    dbname = File.basename(path_to_windows, '.*')

    redis_port = find_available_port
    spin_up_redis(redis_port, dbname)
    sleep 5
    puts redis_port

    redis = Redis.new(:host => "localhost", :port => redis_port, :db => 1)
    redis.ping
    puts "PONG, redis server found"

    ## first lets deal with the 32-mer windows
    path_output = File.dirname(path_to_windows)

    ## split data into 33 batches
    puts "splitting into 33 batches"
    i = LibrarySplitter.create(:sge_library_splitter)
    files = i.split_data(path_to_windows, :bed)

    ## thread-pool seems to skip loops that only consist of a single element special case
    thread_pool = Thread.pool(11)
    files.each_with_index do |file, i|
      thread_pool.process {
        puts "Processing file #{i}"
        ## send each batch to SGE with 'ruby lib/run_redis_client.rb redis_host=localhost redis_port=6379 path_to_file=splitted_file
        opts = SGECommandOptions.new({:job_name => "RedisFiller", :hostname_filter => "*", :logs_dir => "#{path_output}/logs"})
        opts.add_module("ruby/1.9.3")
        s = SGECommand.run_with_options("ruby #{File.expand_path(File.dirname(__FILE__))}/run_redis_client.rb redis_host=localhost redis_port=#{redis_port} path_to_file=#{file} mode=write", opts)
      }
      sleep 2
    end

    puts "waiting for initial write to finish.."
    thread_pool.shutdown

    ## first lets deal with the 32-mer windows
    path_output = File.dirname(path_to_anchors)

    ## split data into 33 batches
    puts "splitting into 33 batches"
    i = LibrarySplitter.create(:sge_library_splitter)
    files = i.split_data(path_to_anchors, :bed)

    ## thread-pool seems to skip loops that only consist of a single element special case
    thread_pool = Thread.pool(11)
    files.each_with_index do |file, i|
      thread_pool.process {
        puts "Processing file #{i}"
        ## send each batch to SGE with 'ruby lib/run_redis_client.rb redis_host=localhost redis_port=6379 path_to_file=splitted_file
        opts = SGECommandOptions.new({:job_name => "RedisFiller", :hostname_filter => "*", :logs_dir => "#{path_output}/logs"})
        opts.add_module("ruby/1.9.3")
        s = SGECommand.run_with_options("ruby #{File.expand_path(File.dirname(__FILE__))}/run_redis_client.rb redis_host=localhost redis_port=#{redis_port} path_to_file=#{file} mode=filter", opts)
      }
      sleep 2
    end

    puts "waiting for filter to finish.."
    thread_pool.shutdown

    ## TODO: parse through redis, dump all keys into bed file (chromosome, location)
    ## once everything is done, shutdown and copy RDB file into another folder
    File.open("results.bed", 'w') do |fOut|
      redis.scan_each(:count => 5000) do |key|
        if redis.get(key).to_i == 0
          chrom = key.split("_").first
          pos = key.split("_").last
          fOut.puts("#{chrom}\t#{pos}")
          redis.set(key, -1)
        end
      end
    end

    redis.shutdown
  end

  def find_available_port
    server = TCPServer.new("127.0.0.1", 0)
    server.addr[1]
  ensure
    server.close if server
  end

  def spin_up_redis(redis_port, dbname)
    @process = ChildProcess.build(File.expand_path("~/bin/redis-server"), "-")
    @process.duplex = true
    @process.start
    at_exit { @process.send(:send_kill) }
    @process.io.stdin.puts config(redis_port, dbname)
    @process.io.stdin.close
  end

  def config(redis_port, dbname)
    <<-CONFIG
    daemonize no
    port #{redis_port || find_available_port}
    dbfilename #{dbname}.db
    dir /clustertmp/brennecke/jurczak/projects/redis_test
    appendonly no
    CONFIG
  end
end
