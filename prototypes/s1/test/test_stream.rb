require 'stream.rb'

class StreamTest < Test::Unit::TestCase
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
        sum = @stream.filter { |tuple| tuple.value.odd? }.sum(:value)
        assert_equal 25, sum.data

        @stream.add(Tuple.new(20, :set => 0, :value => 25))
        assert_equal 50, sum.data
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


    def test_windowed
        other = @stream[4.s] >> map { |elem| elem[:value] }
        assert_equal [7, 8, 9], other.data

        Clock.instance.advance 1

        assert_equal [8, 9], other.data
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
        min_per_set = @stream[11.s].groupby(:set) { |stream| stream.min(:value) }
        assert_equal 0, min_per_set[0].data
        assert_equal 1, min_per_set[1].data
        assert_equal 2, min_per_set[2].data

        higher_two_per_set = @stream[11.s].partitionby(:set) { |stream| stream.sort(:value).last(2) }
        assert_equal [4, 5, 6, 7, 8, 9], higher_two_per_set.data.map { |tuple| tuple.value }

        sorted_by_set_and_value = @stream[11.s].sort(:set, :value)
        assert_equal [0, 3, 6, 9, 1, 4, 7, 2, 5, 8], sorted_by_set_and_value.data.map { |tuple| tuple.value }

        Clock.instance.advance 5

        assert_equal 6, min_per_set[0].data
        assert_equal [5, 6, 7, 8, 9], higher_two_per_set.data.map { |tuple| tuple.value }
        assert_equal [6, 9, 7, 5, 8], sorted_by_set_and_value.data.map { |tuple| tuple.value }
    end


    def test_complex_query
        avg_value = @stream[11.s].avg(:value)

        value_equals_avg = @stream.partitionby(:set) { |stream| stream.filter { |tuple| tuple.value == avg_value.data } }
        assert_equal 4, avg_value.data
        assert_equal [avg_value.data], value_equals_avg.data.map { |tuple| tuple.value }

        @stream.add(Tuple.new(12, :value => 12, :set => 1))
        assert_equal 5, avg_value.data
        assert_equal [avg_value.data], value_equals_avg.data.map { |tuple| tuple.value }

        value_equals_avg_in_group = @stream[11.s].partitionby(:set) { |stream| stream.filter { |tuple| tuple.value == stream.min(:value).data } }
        assert_equal [0, 1, 2], value_equals_avg_in_group.data.map { |tuple| tuple.value }

        assert_equal 1, value_equals_avg_in_group.avg(:value).data

        Clock.instance.advance 5

        assert_equal (5+6+7+8+9+12)/6, avg_value.data
        assert_equal 6, value_equals_avg_in_group.avg(:value).data
    end


    def test_join
        streamb = Stream.new
        10.times { |n| streamb.add(Tuple.new(n, :set => n % 3, :id => n)) }

        joined = @stream.join(streamb, :value, :id)
        assert_equal [0, 2, 4, 6, 8, 10, 12, 14, 16, 18], joined.data.map { |tuple| [tuple[:value], tuple[:id]] }.map { |a, b| a + b }
    end
end
