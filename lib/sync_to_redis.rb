require 'shell_command'
require 'redis'

class SyncToRedis
  def convert_to_frequency_files(path_to_bed)
    path_to_uniq_file = path_to_bed.gsub(".split", ".uniq")
    cmdline = "cat #{path_to_bed} | ruby -nae 'puts $F[3]' | uniq -c > #{path_to_uniq_file}"
    cmd = ShellCommand.run(cmdline)

    return path_to_uniq_file
  end

  def push_frequencies_to_redis(redis_connection, file_input)
    file_input.each_with_index do |line, i|
      line = line.strip
      fields = line.split(" ")
      title = fields[1].split("_")[0..-2].join("_")
      count = fields[0].to_i

      redis_connection.incrby(title, count)
    end
  end

  def remove_frequences_from_redis(redis_connection, file_input)
    file_input.each_with_index do |line, i|
      line = line.strip
      fields = line.split(" ")
      title = fields[1].split("_")[0..-2].join("_")
      count = fields[0].to_i

      value = redis_connection.decrby(title, count)
      redis_connection.del(title) if value.to_i < 0
    end
  end
end
