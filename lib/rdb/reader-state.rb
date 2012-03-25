module RDB
  class ReaderState
    attr_accessor :database, :key, :type, :expiration, :info
    attr_reader :callbacks, :filter

    def initialize(callbacks = nil, filter = nil)
      @callbacks = callbacks || EmptyCallbacks.new
      @filter = filter
    end

    def key_expires?
      !@expiration.nil?
    end

    def mnemonic_type
      case @type
      when Type::STRING then :string
      when Type::SET, Type::SET_INTSET then :set
      when Type::LIST, Type::LIST_ZIPLIST then :list
      when Type::ZSET, Type::ZSET_ZIPLIST then :sortedset
      when Type::HASH, Type::HASH_ZIPMAP, Type::HASH_ZIPLIST then :hash
      end
    end
  end
end
