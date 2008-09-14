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
        sum = @stream.sum(:value)
        assert_equal 45, sum.data

        @stream.add(Tuple.new(20, :set => 0, :value => 200))
        assert_equal 245, sum.data
    end


    def test_groupby
        other = @stream.groupby(:set) { |stream| stream.sum(:value) }
        assert_equal 18, other[0].data
        assert_equal 12, other[1].data
        assert_equal 15, other[2].data

        other = @stream >> groupby(:set) { |stream| stream.sum(:value) }
        assert_equal 18, other[0].data
        assert_equal 12, other[1].data
        assert_equal 15, other[2].data

        @stream.add(Tuple.new(20, :set => 0, :value => 200))
        assert_equal 218, other[0].data
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


    def test_last
        last_stream = @stream.partitionby(:set) { |stream| stream.filter { |tuple| tuple.value.odd? } >> map { |tuple| tuple.select :value => tuple.value ** 2 } }.last
        assert_equal 81, last_stream.data.value

        @stream.add(Tuple.new(11, :value => 2, :set => 1))
        assert_equal 81, last_stream.data.value

        @stream.add(Tuple.new(12, :value => 3, :set => 1))
        assert_equal 9, last_stream.data.value
    end


    def test_sort
        min_per_set = @stream.groupby(:set) { |stream| stream.min(:value) }
        assert_equal 0, min_per_set[0].data
        assert_equal 1, min_per_set[1].data
        assert_equal 2, min_per_set[2].data
    end
end
