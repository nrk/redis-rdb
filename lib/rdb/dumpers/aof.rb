module RDB
  module Dumpers
    #
    # TODO: we should actually make the dumper configurable with the
    # level of compatibility of the AOF file being produced against
    # a specific Redis version. For example PEXPIREAT is unsupported
    # on Redis <= 2.4. Also, Redis >= 2.4 can ingest AOF files using
    # variadic LPUSH, SADD and ZADD.
    #
    class AOF
      include Dumper

      def start_database(database)
        self << serialize_command(:select, [database])
      end

      def pexpireat(key, expiration, state)
        self << serialize_command(:pexpireat, [key, expiration])
      end

      def set(key, value, state)
        self << serialize_command(:set, [key, value])
      end

      def rpush(key, value, state)
        self << serialize_command(:rpush, [key, value])
      end

      def sadd(key, value, state)
        self << serialize_command(:sadd, [key, value])
      end

      def zadd(key, score, value, state)
        self << serialize_command(:zadd, [key, score, value])
      end

      def hset(key, field, value, state)
        self << serialize_command(:hset, [key, field, value])
      end

      def serialize_command(command, arguments)
        buffer = "*#{arguments.length + 1}\r\n$#{command.length}\r\n#{command.upcase}\r\n"
        buffer << arguments.map { |arg| "$#{arg.to_s.length}\r\n#{arg}\r\n" }.join
      end
    end
  end
end
