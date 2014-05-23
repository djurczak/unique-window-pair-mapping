require 'redis'
require 'thread/pool'
require 'childprocess'

require_relative 'sge_command.rb'
require_relative 'library_splitter.rb'

class RedisServer
  attr_reader :process, :redis_port

  def perform(path_to_windows, path_to_anchors, path_to_results)
    ## spin up new redis instance
    dbname = File.basename(path_to_results, '.*')

    redis_port = find_available_port
    spin_up_redis(redis_port, File.dirname(path_to_results), dbname)
    sleep 5
    puts redis_port

    redis = Redis.new(:host => "localhost", :port => redis_port, :db => 1)
    redis.ping
    puts "PONG, redis server found"

    ## first lets deal with the 32-mer windows
    path_output = File.dirname(path_to_results)

    ## split data into 33 batches
    puts "splitting into 33 batches"
    i = LibrarySplitter.create(:sge_library_splitter)
    files = i.split_data(path_to_windows, :bed)

    orientation = "forward"
    orientation = "reverse" if ["rev", "custom"].include?(dbname) 

    ## REMEMBER:
    ## thread-pool seems to skip loops that only consist of a single element
    thread_pool = Thread.pool(11)
    files.each_with_index do |file, i|
      thread_pool.process {
        puts "Processing file #{i}"
        puts "(#{file})"

        ## send each batch to SGE with:
        ## 'ruby lib/run_redis_client.rb redis_host=localhost redis_port=6379
        ## path_to_file=splitted_file'
        opts = SGECommandOptions.new({
          :job_name => "RedisFiller",
          :hostname_filter => "*",
          :logs_dir => "#{path_output}/logs"
        })
        opts.add_module("ruby/1.9.3")

        s = SGECommand.run_with_options("ruby #{File.expand_path(File.dirname(__FILE__))}/run_redis_client.rb redis_host=localhost redis_port=#{redis_port} path_to_file=#{file} mode=write orientation=#{orientation}", opts)
      }
      sleep 2
    end

    puts "waiting for initial write to finish.."
    thread_pool.shutdown

    ## first lets deal with the 32-mer windows
    # path_output = File.dirname(path_to_anchors)

    ## split data into 33 batches
    puts "splitting into 33 batches"
    i = LibrarySplitter.create(:sge_library_splitter)
    files = i.split_data(path_to_anchors, :bed)

    ## WARNING:
    ## thread-pool seems to skip loops that only consist of a single element
    thread_pool = Thread.pool(11)
    files.each_with_index do |file, i|
      thread_pool.process {
        puts "Processing file #{i}"
        ## send each batch to SGE with:
        ## 'ruby lib/run_redis_client.rb redis_host=localhost redis_port=6379
        ## path_to_file=splitted_file'
        opts = SGECommandOptions.new({
          :job_name => "RedisFilter",
          :hostname_filter => "*",
          :logs_dir => "#{path_output}/logs"
        })
        opts.add_module("ruby/1.9.3")

        s = SGECommand.run_with_options("ruby #{File.expand_path(File.dirname(__FILE__))}/run_redis_client.rb redis_host=localhost redis_port=#{redis_port} path_to_file=#{file} mode=filter orientation=#{orientation}", opts)
      }
      sleep 2
    end

    puts "waiting for filter to finish.."
    thread_pool.shutdown

    ## parse through redis, dump all keys into bed file (chromosome, location)
    ## once everything is done, shutdown and copy RDB file into another folder
    File.open(path_to_results, 'w') do |fOut|
      ## FIXME: scan_each on one node is extremely slow, lets check if we can
      ## just run a pop on multiple nodes at the same time and then concat the
      ## resulting files together
      redis.scan_each(:count => 50000) do |key|
        if redis.get(key).to_i == 0
          chrom = key.split("_").first
          pos = key.split("_").last
          fOut.puts("#{chrom}\t#{pos}\t#{pos.to_i+1}")
          redis.set(key, 0)
        end
      end
    end

    begin
      redis.shutdown
    rescue => e
      puts e
      iterations = 0
      while(@process.exited? == false) do
        iterations += 1
        puts "waiting for redis to finish saving (iteration #{iterations})"
        sleep(60)
      end
    end
  end

  ## easiest way to find still available port in UNIX
  ## http://stackoverflow.com/a/201528
  def find_available_port
    server = TCPServer.new("127.0.0.1", 0)
    server.addr[1]
  ensure
    server.close if server
  end

  ## create a new redis-server instance which will get automatically killed
  ## once our script finishes. Send in a dynamically generated config via an
  ## input pipe..
  def spin_up_redis(redis_port, redis_path, dbname)
    @process = ChildProcess.build(File.expand_path("~/bin/redis-server"), "-")
    @process.duplex = true
    @process.start
    at_exit { @process.send(:send_kill) if !@process.exited? }
    @process.io.stdin.puts config(redis_port, redis_path, dbname)
    @process.io.stdin.close
  end

  ## generate redis config dynamically..
  def config(redis_port, redis_path, dbname)
    <<-CONFIG
    daemonize no
    port #{redis_port || find_available_port}
    dbfilename #{dbname}.db
    dir #{redis_path}
    appendonly no
    save 3600 1000
    CONFIG
  end
end
