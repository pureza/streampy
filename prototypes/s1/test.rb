#! /usr/bin/ruby

require 'test/unit'
require 'stream.rb'

class TC_MyTest < Test::Unit::TestCase
    def setup
        @stream = Stream.new
        10.times { |n| @stream.add(Tuple.new(n, :set => n % 3, :value => n)) }
    end

    def test_map
        other = @stream.map { |elem| elem[:value] * 2 }
        assert_equal [0, 2, 4, 6, 8, 10, 12, 14, 16, 18], other.data
    end


    def test_filter
        other = @stream.filter { |elem| elem[:value].odd? }.map { |elem| elem[:value] }
        assert_equal [1, 3, 5, 7, 9], other.data
    end


    def test_fold
        assert_equal 45, @stream.sum(:value)
    end


    def test_groupby
        other = @stream.groupby(:set) { |stream| stream.sum(:value) }
        assert_equal 18, other[[0]]
        assert_equal 12, other[[1]]
        assert_equal 15, other[[2]]
    end

end
