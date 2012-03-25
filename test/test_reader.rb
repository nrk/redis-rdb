$:.unshift File.expand_path('../test', File.dirname(__FILE__)),
           File.expand_path('../lib', File.dirname(__FILE__))

require 'rdb'
require 'helpers'

setup do
  {
    callbacks: TestCallbacks.new
  }
end

test 'should handle empty databases' do |options|
  rdb = read_test_rdb('database_empty.rdb', options)

  events = [
    [:start_rdb, [3]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end

test 'should handle multiple logical databases' do |options|
  rdb = read_test_rdb('database_multiple_logical_dbs.rdb', options)

  events = [
    [:start_rdb, [3]],
    [:start_database, [0]],
    [:set, ['key_in_zeroth_database', 'zero']],
    [:end_database, [0]],
    [:start_database, [2]],
    [:set, ['key_in_second_database', 'second']],
    [:end_database, [2]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end

test 'should read integer keys' do |options|
  rdb = read_test_rdb('keys_integer.rdb', options)

  events = [
    [:start_rdb, [3]],
    [:start_database, [0]],
    [:set, [183358245, 'Positive 32 bit integer']],
    [:set, [125, 'Positive 8 bit integer']],
    [:set, [-29477, 'Negative 16 bit integer']],
    [:set, [-123, 'Negative 8 bit integer']],
    [:set, [43947, 'Positive 16 bit integer']],
    [:set, [-183358245, 'Negative 32 bit integer']],
    [:end_database, [0]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end

test 'should read keys and their expiration' do |options|
  rdb = read_test_rdb('keys_with_expiration.rdb', options)

  events = [
    [:start_rdb, [3]],
    [:start_database, [0]],
    [:set, ['expires_ms_precision', '2022-12-25 10:11:12.000573']],
    [:pexpireat, ['expires_ms_precision', 1671943272573000]],
    [:end_database, [0]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end

test 'should read LZF-compressed key strings' do |options|
  rdb = read_test_rdb('keys_compressed.rdb', options)

  events = [
    [:start_rdb, [3]],
    [:start_database, [0]],
    [:set, ['a' * 200, 'Key that redis should compress easily']],
    [:end_database, [0]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end

test 'should read uncompressed key strings' do |options|
  rdb = read_test_rdb('keys_uncompressed.rdb', options)

  events = [
    [:start_rdb, [3]],
    [:start_database, [0]],
    [:set, [RDB_KEY_MIN_4BITS_MAX_16BITS, 'Key length more than 6 bits but less than 14 bits']],
    [:set, [RDB_KEY_MAX_6BITS, 'Key length within 6 bits']],
    [:set, [RDB_KEY_MIN_14BITS_MAX_32BITS, 'Key length more than 14 bits but less than 32']],
    [:end_database, [0]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end

test 'should read lists' do |options|
  rdb = read_test_rdb('list_normal.rdb', options)

  events = [:start_rdb, :start_database, :start_list, *([:rpush] * 1000), :end_list, :end_database, :end_rdb]

  assert events == rdb.events.map { |event,| event }
  assert 1000 == rdb.lists['force_linkedlist'].length
  assert '41PJSO2KRV6SK1WJ6936L06YQDPV68R5J2TAZO3YAR5IL5GUI8' == rdb.lists['force_linkedlist'][0]
  assert 'E1RVJE0CPK9109Q3LO6X4D1GNUG5NGTQNCYTJHHW4XEM7VSO6V' == rdb.lists['force_linkedlist'][499]
  assert '2C5URE2L24D9GJUZJ59IWCAH8SGYF5T7QZ0EXQ0IE4I2JSB1QD' == rdb.lists['force_linkedlist'][999]
end

test 'should read lists with integers serialized as ziplists' do |options|
  rdb = read_test_rdb('list_of_integers_as_ziplist.rdb', options)

  events = [
    [:start_rdb, [3]],
    [:start_database, [0]],
    [:start_list, ['ziplist_with_integers', 4]],
    [:rpush, ['ziplist_with_integers', 63]],
    [:rpush, ['ziplist_with_integers', 16380]],
    [:rpush, ['ziplist_with_integers', 65535]],
    [:rpush, ['ziplist_with_integers', 9223372036854775807]],
    [:end_list, ['ziplist_with_integers']],
    [:end_database, [0]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end

test 'should read lists with compressed strings serialized as ziplists' do |options|
  rdb = read_test_rdb('list_of_compressed_strings_as_ziplist.rdb', options)

  events = [
    [:start_rdb, [3]],
    [:start_database, [0]],
    [:start_list, ['ziplist_compresses_easily', 6]],
    [:rpush, ['ziplist_compresses_easily', 'a' * 6]],
    [:rpush, ['ziplist_compresses_easily', 'a' * 12]],
    [:rpush, ['ziplist_compresses_easily', 'a' * 18]],
    [:rpush, ['ziplist_compresses_easily', 'a' * 24]],
    [:rpush, ['ziplist_compresses_easily', 'a' * 30]],
    [:rpush, ['ziplist_compresses_easily', 'a' * 36]],
    [:end_list, ['ziplist_compresses_easily']],
    [:end_database, [0]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end

test 'should read lists with uncompressed strings serialized as ziplists' do |options|
  rdb = read_test_rdb('list_of_uncompressed_strings_as_ziplist.rdb', options)

  events = [
    [:start_rdb, [3]],
    [:start_database, [0]],
    [:start_list, ['ziplist_doesnt_compress', 2]],
    [:rpush, ['ziplist_doesnt_compress', 'aj2410']],
    [:rpush, ['ziplist_doesnt_compress', 'cc953a17a8e096e76a44169ad3f9ac87c5f8248a403274416179aa9fbd852344']],
    [:end_list, ['ziplist_doesnt_compress']],
    [:end_database, [0]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end

test 'should read sets' do |options|
  rdb = read_test_rdb('set_normal.rdb', options)

  events = [
    [:start_rdb, [3]],
    [:start_database, [0]],
    [:start_set, ['regular_set', 6]],
    [:sadd, ['regular_set', 'beta']],
    [:sadd, ['regular_set', 'delta']],
    [:sadd, ['regular_set', 'alpha']],
    [:sadd, ['regular_set', 'phi']],
    [:sadd, ['regular_set', 'gamma']],
    [:sadd, ['regular_set', 'kappa']],
    [:end_set, ['regular_set']],
    [:end_database, [0]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end

test 'should read sets encoded as intsets (16 bits)' do |options|
  rdb = read_test_rdb('set_as_intset_16bits.rdb', options)

  events = [
    [:start_rdb, [3]],
    [:start_database, [0]],
    [:start_set, ['intset_16', 3]],
    [:sadd, ['intset_16', 32764]],
    [:sadd, ['intset_16', 32765]],
    [:sadd, ['intset_16', 32766]],
    [:end_set, ['intset_16']],
    [:end_database, [0]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end

test 'should read sets encoded as intsets (32 bits)' do |options|
  rdb = read_test_rdb('set_as_intset_32bits.rdb', options)

  events = [
    [:start_rdb, [3]],
    [:start_database, [0]],
    [:start_set, ['intset_32', 3]],
    [:sadd, ['intset_32', 2147418108]],
    [:sadd, ['intset_32', 2147418109]],
    [:sadd, ['intset_32', 2147418110]],
    [:end_set, ['intset_32']],
    [:end_database, [0]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end

test 'should read sets encoded as intsets (64 bits)' do |options|
  rdb = read_test_rdb('set_as_intset_64bits.rdb', options)

  events = [
    [:start_rdb, [3]],
    [:start_database, [0]],
    [:start_set, ['intset_64', 3]],
    [:sadd, ['intset_64', 9223090557583032316]],
    [:sadd, ['intset_64', 9223090557583032317]],
    [:sadd, ['intset_64', 9223090557583032318]],
    [:end_set, ['intset_64']],
    [:end_database, [0]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end

test 'should read sorted sets' do |options|
  rdb = read_test_rdb('sortedset_normal.rdb', options)

  events = [:start_rdb, :start_database, :start_sortedset, *([:zadd] * 500), :end_sortedset, :end_database, :end_rdb]

  assert events == rdb.events.map { |event,| event }
  assert 500 == rdb.sortedsets['force_sorted_set'].length
  assert ['3.1899999999999999', 'G72TWVWH0DY782VG0H8VVAR8RNO7BS9QGOHTZFJU67X7L0Z3PR'] == rdb.sortedsets['force_sorted_set'][0]
  assert ['4.3499999999999996', '95S5BW6RTTCUIQXOTT77YQC9D1ULUSB8MPYU71Q32WMLAL7WWG'] == rdb.sortedsets['force_sorted_set'][249]
  assert ['4.7300000000000004', 'MBNE4KFV66LQQUZNFC7Z5KS1Y5I1IIIOT37OBUSGNDQQ2ITGZ8'] == rdb.sortedsets['force_sorted_set'][499]
end

test 'should read sorted sets encoded as ziplists' do |options|
  rdb = read_test_rdb('sortedset_as_ziplist.rdb', options)

  events = [
    [:start_rdb, [3]],
    [:start_database, [0]],
    [:start_sortedset, ['sorted_set_as_ziplist', 3]],
    [:zadd, ['sorted_set_as_ziplist', 1, '8b6ba6718a786daefa69438148361901']],
    [:zadd, ['sorted_set_as_ziplist', '2.3700000000000001', 'cb7a24bb7528f934b841b34c3a73e0c7']],
    [:zadd, ['sorted_set_as_ziplist', '3.423', '523af537946b79c4f8369ed39ba78605']],
    [:end_sortedset, ['sorted_set_as_ziplist']],
    [:end_database, [0]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end

test 'should read hashes' do |options|
  rdb = read_test_rdb('hash_normal.rdb', options)

  events = [:start_rdb, :start_database, :start_hash, *([:hset] * 1000), :end_hash, :end_database, :end_rdb]

  assert events == rdb.events.map { |event,| event }
  assert 1000 == rdb.hashes['force_dictionary'].length
  assert 'MBW4JW2398Z1DLMAVE5MAK8Z368PJIEHC7WGJUMTPX96KGWFRM' == rdb.hashes['force_dictionary']['N8HKPIK4RC4I2CXVV90LQCWODW1DZYD0DA26R8V5QP7UR511M8']
  assert 'MFR2P9FJS90TS3S23QISM2HU691ZL4DTDP2I4ABBLNCFZI79DR' == rdb.hashes['force_dictionary']['8W7OAWM5W3ED3I4AUBC600IU4S67UGV6M91AOWW1STH129NBMO']
  assert '4YOEJ3QPNQ6UADK4RZ3LDN8H0KQHD9605OQTJND8B1FTODSL74' == rdb.hashes['force_dictionary']['PET9GLTADHF2LAE6EUNDX6SPE1M7VFWBK5S9TW3967SAG0UUUB']
end

test 'should read hashes encoded as ziplists' do |options|
  rdb = read_test_rdb('hash_as_ziplist.rdb', options)

  events = [
    [:start_rdb, [4]],
    [:start_database, [0]],
    [:start_hash, ['zipmap_compresses_easily', 3]],
    [:hset, ['zipmap_compresses_easily', 'a', 'aa']],
    [:hset, ['zipmap_compresses_easily', 'aa', 'aaaa']],
    [:hset, ['zipmap_compresses_easily', 'aaaaa', 'aaaaaaaaaaaaaa']],
    [:end_hash, ['zipmap_compresses_easily']],
    [:end_database, [0]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end

test 'should read hashes with compressed strings encoded as zipmaps' do |options|
  rdb = read_test_rdb('hash_with_compressed_strings_as_zipmap.rdb', options)

  events = [
    [:start_rdb, [3]],
    [:start_database, [0]],
    [:start_hash, ['zipmap_compresses_easily', 3]],
    [:hset, ['zipmap_compresses_easily', 'a', 'aa']],
    [:hset, ['zipmap_compresses_easily', 'aa', 'aaaa']],
    [:hset, ['zipmap_compresses_easily', 'aaaaa', 'aaaaaaaaaaaaaa']],
    [:end_hash, ['zipmap_compresses_easily']],
    [:end_database, [0]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end

test 'should read hashes with uncompressed strings encoded as zipmaps' do |options|
  rdb = read_test_rdb('hash_with_uncompressed_strings_as_zipmap.rdb', options)

  events = [
    [:start_rdb, [3]],
    [:start_database, [0]],
    [:start_hash, ['zimap_doesnt_compress', 2]],
    [:hset, ['zimap_doesnt_compress', 'MKD1G6', '2']],
    [:hset, ['zimap_doesnt_compress', 'YNNXK', 'F7TI']],
    [:end_hash, ['zimap_doesnt_compress']],
    [:end_database, [0]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end

test 'should filter top-level objects before raising events' do |options|
  options[:callbacks].filter = lambda do |state|
    state.database == 2 && state.key.match(/second/) && state.mnemonic_type == :string
  end

  rdb = read_test_rdb('database_multiple_logical_dbs.rdb', options)

  events = [
    [:start_rdb, [3]],
    [:start_database, [0]],
    [:skip_object, ['key_in_zeroth_database']],
    [:end_database, [0]],
    [:start_database, [2]],
    [:set, ['key_in_second_database', 'second']],
    [:end_database, [2]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end
