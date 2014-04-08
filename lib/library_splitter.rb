require 'rubygems'
require 'fileutils'
require 'yell'

require_relative './sge_command.rb'
require_relative './combine_data.rb'

class LibrarySplitter
  attr_accessor :tmp_base_path, :tmp_folder_name_length, :num_parallel
  attr_accessor :tmp_path
  @@subclasses = { }

  def initialize
    @tmp_base_path = '/clustertmp/brennecke/apps/splitting/'
    @tmp_folder_name_length = 15
    @num_parallel = 33

    logger = Yell.new do |l|
      l.name = "logger"
      l.adapter STDOUT, :level => [:info, :fatal], :format => false
    end
  end

  def self.create(type)
    c = @@subclasses[type]
    return c.new if c
    raise "Unknown Library Splitter"
  end

  def self.register_class(name)
    @@subclasses[name] = self
  end
end

class SGELibrarySplitter < LibrarySplitter

  def combine_data(path, data_suffix, data_format = :generic)
    splitted_files = Dir["#{path}/*"].select { |s| s.match(/#{data_suffix}$/)}
    raise "Can't find any files matching suffix #{data_suffix} in directory #{path}" if splitted_files.count == 0
    tmp_folder = File.dirname(splitted_files.first)

    instance = CombineData.create(data_format)
    instance.perform(splitted_files, tmp_folder, data_suffix)
  end

  def split_data(path_to_data, data_format)
    random_folder = (36**(@tmp_folder_name_length-1) + rand(36**@tmp_folder_name_length - 36**(@tmp_folder_name_length-1))).to_s(36)
    @tmp_path = @tmp_base_path + random_folder

    FileUtils.mkpath(@tmp_path)
    FileUtils.mkpath(@tmp_path + "/logs")

    num_reads = determine_data_length(path_to_data, data_format, @tmp_path)
    num_split_lines = (num_reads/(1.0*@num_parallel)).ceil * DivideByFormat.determine_divisor(data_format)
    split_into_separate_files(path_to_data, num_split_lines, @tmp_path)

    ## return all files in the temp directory where their file extensions matches 'split'
    #@tmp_path = "/clustertmp/brennecke/apps/splitting/ykex0cwl8w1m18x/"
    return Dir[@tmp_path+"/*"].select { |s| File.extname(s).match(/split/) }
  end

  def clean_data

  end

  private
  def split_into_separate_files(path_to_data, num_split_lines, tmp_path)
    root_path = File.expand_path(tmp_path)

    cmdline = "split -l #{num_split_lines} #{path_to_data} #{tmp_path}/#{File.basename(path_to_data, '.*')}.split"
    opts = SGECommandOptions.new({:job_name => "LibSplit-split", :logs_dir => "#{root_path}/logs"})
    s = SGECommand.run_with_options(cmdline, opts)
  end

  def determine_data_length(path_to_data, data_format, tmp_path)
    root_path = File.expand_path(tmp_path)
    line_divisor = DivideByFormat.determine_divisor(data_format)

    cmdline = "wc -l #{path_to_data}"
    opts = SGECommandOptions.new({:job_name => "LibSplit-cnt", :logs_dir => "#{root_path}/logs"})
    s = SGECommand.run_with_options(cmdline, opts)

    matches = s.sout.match(/^(\d+)/)
    raise "Can't find number of lines in output" if matches == nil
    num_lines = matches[1].to_i

    return num_lines/line_divisor
  end

  register_class(:sge_library_splitter)
end

class DivideByFormat
  def self.determine_divisor(data_format)
    data_format = data_format.to_sym

    if data_format == :bed
      return 1
    elsif data_format == :fasta
      return 2
    elsif data_format == :fastq
      return 4
    else
      raise "Don't know the format"
    end
  end
end
