module RDB
  class ReaderState
    attr_accessor :database, :info
    attr_accessor :key, :key_type_id, :key_expiration
    attr_reader :callbacks

    def initialize(callbacks = nil)
      @callbacks = callbacks || EmptyCallbacks.new
    end

    def key_expires?
      !@key_expiration.nil?
    end

    def key_type
      case @key_type_id
      when Type::STRING then :string
      when Type::SET, Type::SET_INTSET then :set
      when Type::LIST, Type::LIST_ZIPLIST then :list
      when Type::ZSET, Type::ZSET_ZIPLIST then :sortedset
      when Type::HASH, Type::HASH_ZIPMAP, Type::HASH_ZIPLIST then :hash
      end
    end
  end
end
