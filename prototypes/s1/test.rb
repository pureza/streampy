#! /usr/bin/ruby

require 'test/unit'
require 'stream.rb'

class TC_MyTest < Test::Unit::TestCase
    def setup
        SimClock.new
        @stream = Stream.new
        10.times { |n| @stream.add(Tuple.new(n, :set => n % 3, :value => n)) }
        Clock.instance.advance 10
    end

    def test_map
        other = @stream.map { |tuple| tuple.select :value => tuple.value * 2, :set => tuple.set }
        assert_equal [0, 2, 4, 6, 8, 10, 12, 14, 16, 18], other.data.map { |tuple| tuple.value }

        other = @stream >> map { |tuple| tuple.select :value => tuple.value * 2, :set => tuple.set }
        assert_equal [0, 2, 4, 6, 8, 10, 12, 14, 16, 18], other.data.map { |tuple| tuple.value }
    end


    def test_filter
        other = @stream.filter { |elem| elem[:value].odd? }
        assert_equal [1, 3, 5, 7, 9], other.data.map { |elem| elem.value }

        other = @stream >> filter { |elem| elem[:value].odd? }
        assert_equal [1, 3, 5, 7, 9], other.data.map { |elem| elem.value }
    end


    def test_fold
        assert_equal 45, @stream.sum(:value)
    end


    def test_groupby
        other = @stream.groupby(:set) { |stream| stream.sum(:value) }
        assert_equal 18, other[0]
        assert_equal 12, other[1]
        assert_equal 15, other[2]

        other = @stream >> groupby(:set) { |stream| stream.sum(:value) }
        assert_equal 18, other[0]
        assert_equal 12, other[1]
        assert_equal 15, other[2]
    end


    def test_partitionby
        other = @stream.partitionby(:set) { |stream| stream.last }
        @stream.add(Tuple.new(20, :set => 0, :value => 200))
        assert_equal [[7, 1], [8, 2], [20, 0]], other.data.map { |tuple| [tuple.timestamp, tuple.set] }

        last_even_set = @stream.partitionby(:set) { |stream| stream.last }.filter { |tuple| tuple.set.even? }
        assert_equal [[8, 2], [20, 0]], last_even_set.data.map { |tuple| [tuple.timestamp, tuple.set] }

        last_even_set = @stream >> partitionby(:set) { |stream| stream.last } >> filter { |tuple| tuple.set.even? }
        assert_equal [[8, 2], [20, 0]], last_even_set.data.map { |tuple| [tuple.timestamp, tuple.set] }
    end


    def test_latest
        other = @stream[4.s] >> map { |elem| elem[:value] }
        assert_equal [6, 7, 8, 9], other.data

        Clock.instance.advance 1

        assert_equal [7, 8, 9], other.data
    end
end
