module RDB
  module Dumpers
    #
    # TODO: PEXPIREAT is supported only for Redis >= 2.6
    #
    class AOF
      include Dumper

      REDIS_AOF_REWRITE_ITEMS_PER_CMD = 64

      def start_database(database)
        self << serialize_command(:select, [database])
      end

      def pexpireat(key, expiration, state)
        self << serialize_command(:pexpireat, [key, expiration])
      end

      def set(key, value, state)
        self << serialize_command(:set, [key, value])
      end

      def start_list(key, length, state)
        reset_buffer(state)
      end

      def rpush(key, member, state)
        handle(:rpush, state, key, member)
      end

      def end_list(key, state)
        flush(:rpush, state)
      end

      def start_set(key, length, state)
        reset_buffer(state)
      end

      def sadd(key, member, state)
        handle(:sadd, state, key, member)
      end

      def end_set(key, state)
        flush(:sadd, state)
      end

      def start_sortedset(key, length, state)
        reset_buffer(state)
      end

      def zadd(key, score, member, state)
        handle(:zadd, state, key, score, member)
      end

      def end_sortedset(key, state)
        flush(:zadd, state)
      end

      def start_hash(key, length, state)
        reset_buffer(state)
      end

      def hset(key, field, value, state)
        handle(variadic? ? :hmset : :hset, state, key, field, value)
      end

      def end_hash(key, state)
        flush(:hmset, state)
      end

      def handle(command, state, key, *arguments)
        if variadic?
          state.info[:buffer].push(*arguments)
          state.info[:queued] += arguments.length
          flush(command, state) if buffer_full?(state)
        else
          self << serialize_command(command, [key, *arguments])
        end
      end

      def flush(command, state)
        if buffer_some?(state)
          self << serialize_command(command, state.info[:buffer])
          reset_buffer(state)
        end
      end

      def serialize_command(command, arguments)
        buffer = "*#{arguments.length + 1}\r\n$#{command.length}\r\n#{command.upcase}\r\n"
        buffer << arguments.map { |arg| "$#{arg.to_s.length}\r\n#{arg}\r\n" }.join
      end

      def variadic?
        @options[:variadic] ||= false
      end

      def reset_buffer(state)
        state.info.merge!({ buffer: [state.key], queued: 0 })
      end

      def buffer_some?(state)
        (state.info[:queued] || 0) > 0
      end

      def buffer_full?(state)
        (state.info[:queued] || 0) == REDIS_AOF_REWRITE_ITEMS_PER_CMD
      end
    end
  end
end
