$:.unshift File.expand_path('../lib', File.dirname(__FILE__))

require 'rdb'

RDB::Reader.read_file('test/rdb/multiple_databases.rdb', callbacks: RDB::DebugCallbacks.new)
