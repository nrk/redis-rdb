module RDB
  class Reader
    class << self
      def read_file(rdb_file, options = {})
        File.open(rdb_file, 'rb') do |rdb|
          read(rdb, options)
        end
      end

      def read(rdb, options = {})
        rdb_version = read_rdb_version(rdb)

        state = ReaderState.new(options[:callbacks], options[:filter])
        state.callbacks.start_rdb(rdb_version)

        loop do
          state.type = rdb.readbyte

          case state.type
          when Opcode::EXPIRETIME_MS
            state.expiration = rdb.read(8).unpack('Q').first
            state.type = rdb.readbyte

          when Opcode::EXPIRETIME
            state.expiration = rdb.read(4).unpack('L').first * 1000
            state.type = rdb.readbyte

          when Opcode::SELECTDB
            state.callbacks.end_database(state.database) unless state.database.nil?
            state.database, = *read_length(rdb)
            state.callbacks.start_database(state.database)
            next

          when Opcode::EOF
            state.callbacks.end_database(state.database) unless state.database.nil?
            state.callbacks.end_rdb()
            break
          end

          state.key = read_string(rdb)

          if state.filter.nil? || state.filter.accept_object?(state)
            read_object(rdb, state)
            notify_expiration(state) if state.key_expires?
          else
            skip_object(rdb, state)
          end

          state.info = nil
        end
      end

      private

      def notify_expiration(state)
        state.callbacks.pexpireat(state.key, state.expiration, state)
        state.expiration = nil
      end

      def read_rdb_version(rdb)
        rdb_header = rdb.read(9)
        signature, version = rdb_header[0..4], rdb_header[5..9].to_i

        raise ReaderError, 'Wrong signature trying to load DB from file' if signature != 'REDIS'
        raise ReaderError, "Can't handle RDB format version #{version}" if version < 1 or version > 4

        version
      end

      def ntohl(rdb)
        value, converted = rdb.read(4).unpack('L').first, 0
        converted = converted | ((value & 0x000000ff) << 24)
        converted = converted | ((value & 0xff000000) >> 24)
        converted = converted | ((value & 0x0000ff00) << 8)
        converted = converted | ((value & 0x00ff0000) >> 8)
        converted
      end

      def read_length(rdb)
        bytes, encoded = rdb.readbyte, false
        encoding = (bytes & 0xC0) >> 6

        [case encoding
         when Length::BITS_6
           bytes & 0x3F
         when Length::BITS_14
           ((bytes & 0x3F) << 8) | rdb.readbyte
         when Length::BITS_32
           ntohl(rdb)
         when Length::ENCODED
           encoded = true
           bytes & 0x3F
         else
           raise ReaderError, "Invalid encoding type for length - #{encoding}"
         end, encoded]
      end

      def read_string(rdb)
        length, encoded = *read_length(rdb)

        if encoded
          case length
          when Encoding::INT8
            rdb.read(1).unpack('c').first
          when Encoding::INT16
            rdb.read(2).unpack('s').first
          when Encoding::INT32
            rdb.read(4).unpack('l').first
          when Encoding::LZF
            compressed_len = read_length(rdb).first
            uncompressed_len = read_length(rdb).first
            LZF.decompress(rdb, compressed_len, uncompressed_len)
          else
            raise ReaderError, "Invalid encoding for string - #{length}"
          end
        else
          rdb.read(length)
        end
      end

      def read_object(rdb, state)
        key, callbacks = state.key, state.callbacks

        case state.type
        when Type::STRING
          state.info = { encoding: :string }
          callbacks.set(key, read_string(rdb), state)

        when Type::LIST
          state.info = { encoding: :linkedlist }
          object_reader(rdb, state) do
            callbacks.rpush(key, read_string(rdb), state)
          end

        when Type::SET
          state.info = { encoding: :hashtable }
          object_reader(rdb, state) do
            callbacks.sadd(key, read_string(rdb), state)
          end

        when Type::ZSET
          state.info = { encoding: :skiplist }
          object_reader(rdb, state) do
            value = read_string(rdb)
            score = rdb.read(rdb.readbyte)
            callbacks.zadd(key, score, value, state)
          end

        when Type::HASH
          state.info = { encoding: :hashtable }
          object_reader(rdb, state) do
            callbacks.hset(key, read_string(rdb), read_string(rdb), state)
          end

        when Type::HASH_ZIPMAP
          read_zipmap(rdb, state)

        when Type::LIST_ZIPLIST
          read_ziplist(rdb, state)

        when Type::SET_INTSET
          read_intset(rdb, state)

        when Type::ZSET_ZIPLIST
          read_zset_from_ziplist(rdb, state)

        when Type::HASH_ZIPLIST
          read_hash_from_ziplist(rdb, state)

        else
          skip_object(rdb, state)

        end
      end

      def object_reader(rdb, state, &block)
        elements = read_length(rdb).first
        state.callbacks.send("start_#{state.mnemonic_type}", state.key, elements, state)
        elements.times do
          block.call(rdb, state)
        end
        state.callbacks.send("end_#{state.mnemonic_type}", state.key, state)
      end

      def read_intset(rdb, state)
        key, callbacks = state.key, state.callbacks
        buffer = StringIO.new(read_string(rdb))

        state.info = { encoding: :intset, encoded_size: buffer.length }
        encoding, entries = *buffer.read(8).unpack('LL')

        callbacks.start_set(key, entries, state)

        entries.times do
          entry = case encoding
                  when 2 then buffer.read(2).unpack('S').first
                  when 4 then buffer.read(4).unpack('L').first
                  when 8 then buffer.read(8).unpack('Q').first
                  else
                    raise ReaderError, "Invalid encoding for intset - #{encoding}"
                  end

          callbacks.sadd(key, entry, state)
        end

        callbacks.end_set(key, state)
      end

      def read_ziplist(rdb, state)
        callbacks = state.callbacks
        ziplist_reader(rdb, state) do |key, buffer|
          callbacks.rpush(key, read_ziplist_entry(buffer, state), state)
        end
      end

      def read_zset_from_ziplist(rdb, state)
        callbacks = state.callbacks
        ziplist_reader_interleaved(rdb, state) do |key, buffer|
          member = read_ziplist_entry(buffer, state)
          score = read_ziplist_entry(buffer, state)
          callbacks.zadd(key, score, member, state)
        end
      end

      def read_hash_from_ziplist(rdb, state)
        callbacks = state.callbacks
        ziplist_reader_interleaved(rdb, state) do |key, buffer|
          field = read_ziplist_entry(buffer, state)
          value = read_ziplist_entry(buffer, state)
          callbacks.hset(key, field, value, state)
        end
      end

      def ziplist_reader_interleaved(rdb, state, &block)
        check_entries = lambda do |entries|
          raise ReaderError, "Expected even number of elements, found #{entries}" if entries.odd?
          entries / 2
        end
        ziplist_reader(rdb, state, check_entries, &block)
      end

      def ziplist_reader(rdb, state, check_entries = nil, &block)
        key, callbacks = state.key, state.callbacks
        buffer = StringIO.new(read_string(rdb))

        state.info = { encoding: :ziplist, encoded_size: buffer.length }
        bytes, offset, entries = *buffer.read(10).unpack('LLS')

        entries = check_entries.call(entries) unless check_entries.nil?
        callbacks.send("start_#{state.mnemonic_type}", key, entries, state)

        entries.times do
          block.call(key, buffer, state)
        end

        if ziplist_end = buffer.readbyte != 255
          raise ReaderError, "Invalid ziplist end - #{ziplist_end}"
        end

        callbacks.send("end_#{state.mnemonic_type}", key, state)
      end

      def read_ziplist_entry(rdb, state)
        previous_length = rdb.readbyte
        if previous_length == 254
          previous_length = rdb.read(4).unpack('L').first
        end

        header = rdb.readbyte
        if header >> 6 == 0
          rdb.read(header & 0x3F)
        elsif header >> 6 == 1
          rdb.read(((header & 0x3F) << 8) | rdb.readbyte)
        elsif header >> 6 == 2
          rdb.read(rdb.read(4).unpack('L').first)
        elsif header >> 4 == 12
          rdb.read(2).unpack('S').first
        elsif header >> 4 == 13
          rdb.read(4).unpack('L').first
        elsif header >> 4 == 14
          rdb.read(8).unpack('Q').first
        else
          raise ReaderError, "Invalid entry header - #{header}"
        end
      end

      def read_zipmap(rdb, state)
        key, callbacks = state.key, state.callbacks
        buffer = StringIO.new(read_string(rdb))
        state.info = { encoding: :zipmap, encoded_size: buffer.length }

        entries = buffer.readbyte

        callbacks.start_hash(key, entries, state)

        loop do
          next_length = read_zipmap_next_length(buffer)
          break if next_length.nil?

          field = buffer.read(next_length)

          next_length = read_zipmap_next_length(buffer)
          break if next_length.nil?

          free, value = buffer.readbyte, buffer.read(next_length)
          buffer.seek(free, IO::SEEK_CUR)

          callbacks.hset(key, field, value, state)
        end

        callbacks.end_hash(key, state)
      end

      def read_zipmap_next_length(rdb)
        length = rdb.readbyte
        case length
        when 1..252 then length
        when 253 then rdb.read(4).unpack('L').first
        when 254 then raise ReaderError, "Unexpected value for length field of zipmap - #{length}"
        else nil
        end
      end

      def skip_object(rdb, state)
        skip = case state.type
               when Type::LIST then read_length(rdb).first
               when Type::SET  then read_length(rdb).first
               when Type::ZSET then read_length(rdb).first * 2
               when Type::HASH then read_length(rdb).first * 2
               when Type::STRING       then 1
               when Type::LIST_ZIPLIST then 1
               when Type::SET_INTSET   then 1
               when Type::ZSET_ZIPLIST then 1
               when Type::HASH_ZIPMAP  then 1
               when Type::HASH_ZIPLIST then 1
               else
                 raise ReaderError, "Trying to skip an unknown object type - #{type}"
               end

        callbacks = state.callbacks

        skip.times do
          skip_string(rdb)
          callbacks.skip_object(state.key, state)
        end
      end

      def skip_string(rdb)
        length, encoded = *read_length(rdb)

        skip = if encoded
          case length
          when Encoding::INT8  then 1
          when Encoding::INT16 then 2
          when Encoding::INT32 then 4
          when Encoding::LZF
            compressed_len = read_length(rdb).first
            uncompressed_len = read_length(rdb).first
            compressed_len
          else
            raise ReaderError, "Invalid encoding for string - #{length}"
          end
        else
          length
        end

        rdb.seek(skip, IO::SEEK_CUR)
      end
    end
  end
end