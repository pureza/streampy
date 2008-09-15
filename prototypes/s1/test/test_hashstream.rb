require 'stream.rb'

class HashStreamTest < Test::Unit::TestCase
    def setup
        SimClock.new
        @stream = Stream.new
        10.times { |n| @stream.add(Tuple.new(n, :set => n % 3, :value => n)) }
        Clock.instance.advance 10
    end

    def test_fold
        other = @stream.groupby(:set) { |stream| stream.sum(:value) }.sum(:value)
    end
end
