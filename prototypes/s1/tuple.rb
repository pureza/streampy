class Tuple
    attr_reader :timestamp
    attr_reader :fields

    def initialize(timestamp, fields)
        @timestamp = timestamp
        @fields = fields
    end

    def [](field)
        if field.nil?
            @fields.values.first
        else
            @fields[field]
        end
    end

    def select(new_fields)
        Tuple.new(@timestamp, new_fields)
    end


    def merge(other)
        Tuple.new([timestamp, other.timestamp].min, @fields.merge(other.fields))
    end


    def method_missing(name)
        raise "Unknown field: #{name}" unless @fields.has_key? name
        @fields[name]
    end


    def to_s
        str = "@#{@timestamp}("
        @fields.each_pair { |k, v| str += "#{k} => #{v}, " }
        str[0..-3] + ")"
    end

    def pretty_print(pp)
        pp.group() do
            pp.text "@#{@timestamp} "
            pp.pp @fields
        end
    end
end
