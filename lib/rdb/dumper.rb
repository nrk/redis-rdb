module RDB
  module Dumper
    include ReaderCallbacks

    def initialize(source, destination, options = {})
      @source = source
      @destination = destination
      @options = options
      @output = nil
    end

    def <<(buffer)
      @output << buffer unless @output.nil?; nil
    end

    def with_streams(&block)
      input = open(@source, 'rb') unless @source.kind_of? IO
      output = open(@destination, 'wb') unless @source.kind_of? IO

      begin
        block.call(input, output)
      rescue
        input.close
        output.close
      end
    end

    def dump
      raise RuntimeException, 'Output stream already opened' if @output

      with_streams do |input, output|
        @output = output
        Reader.read(input, callbacks: self)
      end
    end
  end
end
