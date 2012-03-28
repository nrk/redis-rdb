module RDB
  module ReaderCallbacks
    def accept_key?(state)
      true
    end

    def start_rdb(rdb_version); end

    def end_rdb(); end

    def start_database(database); end

    def end_database(database); end

    def pexpireat(key, expiration, state); end

    def set(key, value, state); end

    def start_list(key, length, state); end

    def rpush(key, value, state); end

    def end_list(key, state); end

    def start_set(key, length, state); end

    def sadd(key, value, state); end

    def end_set(key, state); end

    def start_sortedset(key, length, state); end

    def zadd(key, score, value, state); end

    def end_sortedset(key, state); end

    def start_hash(key, length, state); end

    def hset(key, field, value, state); end

    def end_hash(key, state); end

    def skip_object(key, state); end
  end

  class EmptyCallbacks
    include ReaderCallbacks
  end

  class DebugCallbacks
    include ReaderCallbacks

    def start_rdb(version)
      puts "Start RDB file - version #{version}"
    end

    def end_rdb()
      puts "Close RDB file"
    end

    def start_database(database)
      puts "Open database #{database}."
    end

    def end_database(database)
      puts "Close database #{database}."
    end

    def pexpireat(key, expiration, state)
      puts "PEXPIREAT \"#{key}\" \"#{expiration}\""
    end

    def set(key, value, state)
      puts "SET \"#{key}\" \"#{value}\""
    end

    def start_list(key, length, state)
      puts "Start list \"#{key}\" of #{length} items."
    end

    def rpush(key, value, state)
      puts "RPUSH \"#{key}\" \"#{value}\""
    end

    def end_list(key, state)
      puts "End list \"#{key}\"."
    end

    def start_set(key, length, state)
      puts "Start set \"#{key}\" of #{length} members."
    end

    def sadd(key, value, state)
      puts "SADD \"#{key}\" \"#{value}\""
    end

    def end_set(key, state)
      puts "End set \"#{key}\"."
    end

    def start_sortedset(key, length, state)
      puts "Start sortedset \"#{key}\" of #{length} members."
    end

    def zadd(key, score, value, state)
      puts "ZADD \"#{key}\" \"#{score}\" \"#{value}\""
    end

    def end_sortedset(key, state)
      puts "End sortedset \"#{key}\"."
    end

    def start_hash(key, length, state)
      puts "Start hash \"#{key}\" of #{length} members."
    end

    def hset(key, field, value, state)
      puts "HSET \"#{key}\" \"#{field}\" \"#{value}\""
    end

    def end_hash(key, state)
      puts "End hash \"#{key}\"."
    end

    def skip_object(key, state)
      puts "Skipping object for key #{key} of type #{state.type}"
    end
  end
end
