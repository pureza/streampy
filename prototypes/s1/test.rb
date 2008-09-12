require 'test/unit'
require 'stream.rb'

class TC_MyTest < Test::Unit::TestCase
    def setup
    end

    def test_map
        @stream = Stream.new
        other = @stream.map { |elem| elem * 2 }

        10.times { |n| @stream.add(n) }

        assert_equal [0, 2, 4, 6, 8, 10, 12, 14, 16, 18], other.data
    end
end
