module RDB
  module LZF
    class << self
      def decompress(rdb, compressed_length, expected_length)
        ipos = opos = 0
        input, output = rdb.read(compressed_length), ' ' * expected_length

        while ipos < compressed_length
          ctrl = input.getbyte(ipos)
          ipos += 1

          if ctrl < 32
            (ctrl + 1).times do
              output.setbyte(opos, input.getbyte(ipos))
              ipos += 1
              opos += 1
            end
          else
            length = ctrl >> 5

            if length == 7
              length = length + input.getbyte(ipos)
              ipos += 1
            end

            reference = opos - ((ctrl & 0x1f) << 8) - input.getbyte(ipos) - 1
            ipos += 1

            (length + 2).times do
              output.setbyte(opos, output.getbyte(reference))
              reference += 1
              opos += 1
            end
          end
        end

        if opos != expected_length
          raise Exception, "LZF Decompression error: expected length #{expected_length} does not match #{opos}"
        end

        output
      end
    end
  end
end
