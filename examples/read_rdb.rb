$:.unshift File.expand_path('../lib', File.dirname(__FILE__))

require 'rdb'

options = {
  callbacks: RDB::DebugCallbacks.new,
}

RDB::Reader.read_file('test/rdb/database_multiple_logical_dbs.rdb', options)
