class Schema

    def initialize
        @fields = []
    end

    def field(value)
        @fields << value
    end

    def fields(*values)
        if values.nil?
            @fields
        else
            @fields += values
        end
    end
end
