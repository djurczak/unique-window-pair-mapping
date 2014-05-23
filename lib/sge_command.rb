require 'yell'
require 'command'

class SGEExecuteError < Exception
end

class SGECommandOptions

  attr_accessor :logger
  attr_accessor :name, :group, :out, :err, :hostname_filter, :cores
  attr_accessor :queues, :modules

  def initialize(options)
    @logs_dir = options[:logs_dir] || "logs/"

    @name = options[:job_name]
    @group = options[:group] || "brenneckegrp"
    @cores = options[:num_cores] || "1"
    @hostname_filter = options[:hostname_filter] || "*"
    @hostname_filter = "piwi"
    ## new cluster only supports allhosts.q (no brennecke.q yet)
    @queues = (options[:queues] || "brennecke.q").gsub(" ", "").split(",")
    @logger = options[:logger_name] || "logger"

    @tmp_name_length = 3
    random_suffix = (36**(@tmp_name_length-1) + rand(36**@tmp_name_length - 36**(@tmp_name_length-1))).to_s(36)

    @out = "#{@logs_dir}/#{@name}.#{Time.now.strftime('%d-%H%M')}.#{random_suffix}.out"
    @err = "#{@logs_dir}/#{@name}.#{Time.now.strftime('%d-%H%M')}.#{random_suffix}.err"
    @sge_script = "#{@logs_dir}/#{@name}.#{Time.now.strftime('%d-%H%M')}.#{random_suffix}.q"

    @modules = Array.new
  end

  def add_module(module_name)
    @modules.push(module_name)
  end

  def get_module_string
    return @modules.join(" ")
  end

  def to_s
    ## groups not supported on new cluster yet
    # cmd = "-N #{@name} -o #{@out} -e #{@err} -P #{@group} -pe orte #{@cores}"
    cmd = "-N #{@name} -o #{@out} -e #{@err}"
    ## piwi has no parallel environment yet, it contains 24 SGE slots
    cmd += "-pe #{@queues.include?("openmpi.q") ? "orte" : "smp"} #{@cores}" unless @hostname_filter.include?("piwi")

    @queues.each { |q|
      cmd = cmd + " -q #{q}"
    }

    cmd = cmd + " -l hostname='#{@hostname_filter}'"

    return cmd
  end

  def to_file(command)
    File.open(@sge_script, 'w') { |fOut|
      fOut.puts "\#$ -S /bin/bash\n"
      @queues.each { |q|
        fOut.puts "\#$ -q #{q}\n"
      }
      ## no group support on new cluster yet
      fOut.puts "\# -P #{@group}\n"
      fOut.puts "\#$ -cwd\n"
      fOut.puts "\#$ -o #{@out}\n"
      fOut.puts "\#$ -e #{@err}\n"
      fOut.puts "\#$ -M jurczak@imp.ac.at\n"
      fOut.puts "\#$ -N #{@name}\n"
      fOut.puts "\#$ -l hostname='#{@hostname_filter}'\n"
      fOut.puts "\#$ -pe #{@queues.include?("openmpi.q") ? "orte" : "smp"} #{@cores}\n\n" unless @hostname_filter.include?("piwi")

      fOut.puts "source /etc/profile.d/modules.sh"
      fOut.puts "echo 'node:' `hostname`"
      fOut.puts "rvm use system"
      fOut.puts "module load #{@modules.join(" ")}" if @modules.count > 0
      fOut.puts command
    }

    return @sge_script
  end

  def get_out_content
    return nil unless File.exists?(@out)

    contents = ""
    File.open(@out, 'r') { |outFile|
      contents = outFile.read
    }
    contents
  end

  def get_err_content
    return nil unless File.exists?(@err)

    contents = ""
    File.open(@err, 'r') { |outFile|
      contents = outFile.read
    }
    contents
  end

  def logger
    @logger
  end
end

class SGECommandStatus
  attr_accessor :success, :sout, :serr

  def initialize(success, sout, serr)
    @success = success
    @sout = sout
    @serr = serr
  end

  def success?
    return @success
  end

  def sout
    return @sout
  end

  def serr
    return @serr
  end
end

class SGECommand

  def self.run(command)
      cmdline = build_cmdline(command)

      logger = Yell.new do |l|
        l.name = "logger"
        l.adapter STDOUT, :level => [:info, :fatal], :format => false
      end

      execute(cmdline, command, logger)
  end

  def self.run_with_logger(command, logger)
      cmdline = build_cmdline(command)
      execute(cmdline, command, logger)
  end

  def self.run_with_options(command, options)
      cmdline = build_cmdline_file(command, options)
      begin
        logger = Yell[options.logger]
      rescue Yell::LoggerNotFound => e
        logger = Yell.new do |l|
          l.name = "logger"
          l.adapter STDOUT, :level => [:info, :fatal], :format => false
        end
      end

      cmd = execute(cmdline, command, logger)

      status = cmd.success?

      if status
        begin
          sout = parse_output(options.get_out_content)
          serr = parse_output(options.get_err_content)

          if sout == nil or serr == nil
            sout = cmd.stdout
            serr = cmd.stderr
            status = false
          end
        end
      else
        sout = cmd.stdout
        serr = cmd.stderr
      end
      s = SGECommandStatus.new(status, sout, serr)
      return s
  end

  private
  def self.parse_output(output)
    return output
  end

  def self.build_cmdline(command, options=nil)
    if options == nil
      cmdline = %Q^source /sw/lenny/etc/brenneckegrp.bash; qsub -v command="#{command}" -sync y #{File.expand_path(File.dirname(__FILE__))}/sge_generic_command.q^
    else
      modules = "eval `modulecmd bash add #{options.get_module_string}`;" if options.get_module_string != ""
      cmdline = %Q^#{modules} qsub #{options.to_s} -v command="#{command}" -sync y #{File.expand_path(File.dirname(__FILE__))}/sge_generic_command.q^
    end

    return cmdline
  end

  def self.build_cmdline_file(command, options)
    file_path = options.to_file(command)
    ## when using -V in qsub its impossible to get modules to run
    cmdline = "eval `modulecmd bash add gridengine`; qsub -sync y #{file_path}"

    return cmdline
  end

  def self.execute(cmdline, command, logger)
      cmd = Command.run(cmdline)
      return cmd
  end
end
