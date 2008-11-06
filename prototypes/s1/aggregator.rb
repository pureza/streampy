class Stream
end

class Aggregator < Stream
    def last
        @data.last
    end
end


class SumAggregator < Aggregator
    def initialize(parent, field)
        super(parent)
        @sum = 0
        @field = field
    end

    def add(tuple, external=true)
        if external
            @sum += tuple[@field]
        end
        super(Tuple.new(tuple.timestamp, :value => @sum))
    end

    def remove(tuple)
        @sum -= tuple[@field]
        add(Tuple.new(Clock.instance.now, :value => @sum), external=false)
    end
end


class AvgAggregator < Aggregator
    def initialize(parent, field)
        super(parent)
        @sum = 0
        @count = 0
        @field = field
    end

    def add(tuple, external=true)
        if external
            @sum += tuple[@field]
            @count += 1
        end
        super(Tuple.new(tuple.timestamp, :value => @sum.to_f / @count))
    end

    def remove(tuple)
        @sum -= tuple[@field]
        @count -= 1
        add(Tuple.new(Clock.instance.now, :value => @sum.to_f / @count), external=false)
    end
end
