require_relative './sge_command.rb'

class CombineData
  @@subclasses = {}

  def initialize
    logger = Yell.new do |l|
      l.name = "logger"
      l.adapter STDOUT, :level => [:info, :fatal], :format => false
    end
  end

  def self.create(type)
    c = @@subclasses[type]
    return c.new if c
    raise "Unknown data format"
  end

  def self.register_class(name)
    @@subclasses[name] = self
  end

  private 
  def combine_files(files, path_output, data_suffix)
    raise "No files with suffix #{data_suffix} at location #{path_output} found." if files.count == 0

    root_path = File.expand_path("#{path_output}/..")

    first = File.basename(files.first)
    full_name = first.split(".split").first

    cmdline = "cat #{files.join(" ")} > #{path_output}/#{full_name}#{data_suffix}.overall"
    cmdline += "; rm -rf #{path_output}/*#{data_suffix}; mv #{path_output}/#{full_name}#{data_suffix}.overall #{path_output}/#{full_name}#{data_suffix}"
    opts = SGECommandOptions.new({:job_name => "LibSplit-combine", :logs_dir => "#{root_path}/logs", :hostname_filter => "compute-4-*|compute-5-*"})
    s = SGECommand.run_with_options(cmdline, opts)
  end
end

class CombineFastqData < CombineData

  def perform(files, path_output, data_suffix = nil)
    combine_files(files.sort!, path_output, data_suffix)
  end

  register_class(:fasta)
end

class CombineFastqData < CombineData

  def perform(files, path_output, data_suffix = nil)
    combine_files(files.sort!, path_output, data_suffix)
  end

  register_class(:fastq)
end

class SAMCombinedData < CombineData

  def perform(files, path_output, data_suffix = nil)
    combine_files(files.sort!, path_output, data_suffix)
    extract_sam_header_lines(path_output, data_suffix)
  end

  private
  def extract_sam_header_lines(path_output, data_suffix)
    files = Dir[path_output+"/*"].select { |s| s.match(/#{data_suffix}$/) }
    raise "Found too many entries: #{files.join(" ").to_s}" if files.count > 1
    root_path = File.expand_path("#{path_output}/..")

    cmdline =  %Q^cat #{files.first} | ruby -nae 'BEGIN{first = true}; puts $_ if $_[0] != "@" or first; first = false if $_[0] != "@";' > #{files.first}.filtered; rm -f #{files.first}; mv #{files.first}.filtered #{files.first}^
    opts = SGECommandOptions.new({:job_name => "LibSplit-SAMcombine", :logs_dir => "#{root_path}/logs", :hostname_filter => "compute-4-*|compute-5-*"})
    opts.add_module("ruby")
    s = SGECommand.run_with_options(cmdline, opts)
  end

  register_class(:sam)
end
