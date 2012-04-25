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
    [:start_rdb, [6]],
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
    [:start_rdb, [4]],
    [:start_database, [0]],
    [:set, ['expires_ms_precision', '2022-12-25 10:11:12.573 UTC']],
    [:pexpireat, ['expires_ms_precision', 1671963072573000]],
    [:end_database, [0]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
  assert Time.parse(rdb.events[2][1][1]) == pexpireat_to_time(rdb.events[3][1][1])
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
    [:start_rdb, [6]],
    [:start_database, [0]],
    [:start_list, ['ziplist_with_integers', 24]],
    [:rpush, ['ziplist_with_integers', 0]],
    [:rpush, ['ziplist_with_integers', 1]],
    [:rpush, ['ziplist_with_integers', 2]],
    [:rpush, ['ziplist_with_integers', 3]],
    [:rpush, ['ziplist_with_integers', 4]],
    [:rpush, ['ziplist_with_integers', 5]],
    [:rpush, ['ziplist_with_integers', 6]],
    [:rpush, ['ziplist_with_integers', 7]],
    [:rpush, ['ziplist_with_integers', 8]],
    [:rpush, ['ziplist_with_integers', 9]],
    [:rpush, ['ziplist_with_integers', 10]],
    [:rpush, ['ziplist_with_integers', 11]],
    [:rpush, ['ziplist_with_integers', 12]],
    [:rpush, ['ziplist_with_integers', -2]],
    [:rpush, ['ziplist_with_integers', 13]],
    [:rpush, ['ziplist_with_integers', 25]],
    [:rpush, ['ziplist_with_integers', -61]],
    [:rpush, ['ziplist_with_integers', 63]],
    [:rpush, ['ziplist_with_integers', 16380]],
    [:rpush, ['ziplist_with_integers', 49536]],
    [:rpush, ['ziplist_with_integers', 16777008]],
    [:rpush, ['ziplist_with_integers', -16773840]],
    [:rpush, ['ziplist_with_integers', 1073741872]],
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

test 'should handle hashes with values between 253 and 255 bytes encoded as zipmaps' do |options|
  rdb = read_test_rdb('hash_with_big_values.rdb', options)

  events = [
    [:start_rdb, [2]],
    [:start_database, [0]],
    [:start_hash, ['zipmap_with_big_values', 4]],
    [:hset, ['zipmap_with_big_values', '253bytes', 'NYKK5QA4TDYJFZH0FCVT39DWI89IH7HV9HV162MULYY9S6H67MGS6YZJ54Q2NISW'+
                                                   '9U69VC6ZK3OJV6J095P0P5YNSEHGCBJGYNZ8BPK3GEFBB8ZMGPT2Y33WNSETHINM'+
                                                   'SZ4VKWUE8CXE0Y9FO7L5ZZ02EO26TLXF5NUQ0KMA98973QY62ZO1M1WDDZNS25F3'+
                                                   '7KGBQ8W4R5V1YJRR2XNSQKZ4VY7GW6X038UYQG30ZM0JY1NNMJ12BKQPF2IDQ']],
    [:hset, ['zipmap_with_big_values', '254bytes', 'IZ3PNCQQV5RG4XOAXDN7IPWJKEK0LWRARBE3393UYD89PSQFC40AG4RCNW2M4YAV'+
                                                   'JR0WD8AVO2F8KFDGUV0TGU8GF8M2HZLZ9RDX6V0XKIOXJJ3EMWQGFEY7E56RAOPT'+
                                                   'A60G6SQRZ59ZBUKA6OMEW3K0LH464C7XKAX3K8AXDUX63VGX99JDCW1W2KTXPQRN'+
                                                   '1R1PY5LXNXPW7AAIYUM2PUKN2YN2MXWS5HR8TPMKYJIFTLK2DNQNGTVAWMULON']],
    [:hset, ['zipmap_with_big_values', '255bytes', '6EUW8XSNBHMEPY991GZVZH4ITUQVKXQYL7UBYS614RDQSE7BDRUW00M6Y4W6WUQB'+
                                                   'DFVHH6V2EIAEQGLV72K4UY7XXKL6K6XH6IN4QVS15GU1AAH9UI40UXEA8IZ5CZRR'+
                                                   'K6SAV3R3X283O2OO9KG4K0DG0HZX1MLFDQHXGCC96M9YUVKXOEC5X35Q4EKET0SD'+
                                                   'FDSBF1QKGAVS9202EL7MP2KPOYAUKU1SZJW5OP30WAPSM9OG97EBHW2XOWGICZG']],
    [:hset, ['zipmap_with_big_values', '300bytes', 'IJXP54329MQ96A2M28QF6SFX3XGNWGAII3M32MSIMR0O478AMZKNXDUYD5JGMHJR'+
                                                   'B9A85RZ3DC3AIS62YSDW2BDJ97IBSH7FKOVFWKJYS7XBMIBX0Z1WNLQRY7D27PFP'+
                                                   'BBGBDFDCKL0FIOBYEADX6G5UK3B0XYMGS0379GRY6F0FY5Q9JUCJLGOGDNNP8XW3'+
                                                   'SJX2L872UJZZL8G871G9THKYQ2WKPFEBIHOOTIGDNWC15NL5324W8FYDP97JHKCS'+
                                                   'MLWXNMSTYIUE7F22ZGR4NZK3T0UTBZ2AFRCT5LMT3P6B']],
    [:end_hash, ['zipmap_with_big_values']],
    [:end_database, [0]],
    [:end_rdb, []],
  ]

  assert events == rdb.events
end

test 'should filter top-level objects before raising events' do |options|
  options[:callbacks].filter = lambda do |state|
    state.database == 2 && state.key.match(/second/) && state.key_type == :string
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
