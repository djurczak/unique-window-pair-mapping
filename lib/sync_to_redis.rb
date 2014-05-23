require 'command'
require 'redis'

class SyncToRedis
  def convert_to_frequency_files(path_to_bed)
    path_to_uniq_file = path_to_bed.gsub(".split", ".uniq")
    cmdline = "cat #{path_to_bed} | ruby -nae 'puts $F[3]' | uniq -c > #{path_to_uniq_file}"
    cmd = Command.run(cmdline)

    return path_to_uniq_file
  end

  ##      38 chr2l_20747989_20748020
  def push_frequencies_to_redis(redis_connection, file_input, opts)
    file_input.each_with_index do |line, i|
      data = line_to_data(line, opts)

      redis_connection.incrby(data.title, data.freq)
    end
  end

  def remove_frequences_from_redis(redis_connection, file_input, opts)
    file_input.each_with_index do |line, i|
      data = line_to_data(line, opts)

      curr_freq = redis_connection.decrby(data.title, data.freq)
      redis_connection.del(data.title) if curr_freq.to_i < 0
    end
  end

  def line_to_data(line, opts={})
    line_data = Struct.new :title, :freq
    data = line_data.new

    fields = line.strip.split(" ")
    data.title = parse_location(fields[1], opts)
    data.freq = fields[0].to_i

    data
  end

  def parse_location(location, opts)
    orientation = opts.fetch(:orientation, :forward)
    if orientation == :forward
      location.split("_")[0..-2].join("_")
    elsif orientation == :reverse
      location.split("_")[0] + "_" + location.split("_")[2]
    else
      "error"
    end
  end
end
