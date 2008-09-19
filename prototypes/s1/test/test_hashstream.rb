require 'stream.rb'

class HashStreamTest < Test::Unit::TestCase
    def setup
        SimClock.new
        @stream = Stream.new
        10.times { |n| @stream.add(Tuple.new(n, :set => n % 3, :value => n)) }
        Clock.instance.advance 10
    end

    def test_fold_groupby
        other = @stream[11.s].groupby(:set) { |stream| stream.sum(:value) }.sum(:value)
        assert_equal 45, other.data

        @stream.add(Tuple.new(20, :set => 0, :value => 200))
        assert_equal 245, other.data
    end


    def test_map_groupby
        other = @stream[11.s].groupby(:set) { |stream| stream.sum(:value) }.map { |tuple| tuple.select :key => tuple.key, :valuex2 => tuple.value * 2 }
        assert_equal [36, 24, 30], other.data.map { |tuple| tuple.valuex2 }

        Clock.instance.advance 2
        assert_equal [36, 22, 30], other.data.map { |tuple| tuple.valuex2 }
    end


    def test_filter_groupby
        other = @stream[11.s].groupby(:set) { |stream| stream.sum(:value) }.filter { |tuple| tuple.value.odd? }
        assert_equal [15], other.data.map { |tuple| tuple.value }

        Clock.instance.advance 3
        assert_equal [11, 13], other.data.map { |tuple| tuple.value }
    end


    def test_sort_groupby
        other = @stream[11.s].groupby(:set) { |stream| stream.sum(:value) }.sort(:value)
        assert_equal [12, 15, 18], other.data.map { |tuple| tuple.value }

        Clock.instance.advance 3
        assert_equal [11, 13, 18], other.data.map { |tuple| tuple.value }
    end


    def test_join_groupby
        other = Stream.new
        10.times { |n| other.add(Tuple.new(n, :set => n % 2, :id => n)) }

        a = @stream[11.s].groupby(:set) { |stream| stream.sum(:value) }
        b = other[11.s].groupby(:set) { |stream| stream.sum(:id) }

        c = a.join(b)

        assert_equal [18, 20], c[0].data
        assert c[3].data.nil?
        assert_equal [[18, 20], [12, 25]], c.data.map { |tuple| tuple.value }
    end
end
