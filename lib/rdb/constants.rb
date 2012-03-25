module RDB
  module Length
    BITS_6 = 0
    BITS_14 = 1
    BITS_32 = 2
    ENCODED = 3
  end

  module Encoding
    INT8 = 0
    INT16 = 1
    INT32 = 2
    LZF = 3
  end

  module Opcode
    EXPIRETIME_MS = 252
    EXPIRETIME = 253
    SELECTDB = 254
    EOF = 255
  end

  module Type
    STRING = 0
    LIST = 1
    SET = 2
    ZSET = 3
    HASH = 4
    HASH_ZIPMAP = 9
    LIST_ZIPLIST = 10
    SET_INTSET = 11
    ZSET_ZIPLIST = 12
    HASH_ZIPLIST = 13
  end
end
