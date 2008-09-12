class Tuple
    attr_reader :timestamp

    def initialize(timestamp, fields)
        @timestamp = timestamp
        @fields = fields
    end

    def [](field)
        @fields[field]
    end

    def to_s
        str = "#{@timestamp}: ("
        @fields.each_pair { |k, v| str += "#{k} => #{v}, " }
        str[0..-3] + ")"
    end
end
