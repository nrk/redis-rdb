$:.unshift File.expand_path('../lib', File.dirname(__FILE__))

require 'rdb'

source = 'test/rdb/database_multiple_logical_dbs.rdb'
destination = File.basename(source, '.rdb') + '.aof'

RDB::Dumpers::AOF.new(source, destination, variadic: true).dump
