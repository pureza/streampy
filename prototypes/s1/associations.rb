class OneToManyAssociation
    attr_reader :attribute, :attribute_id, :class

    def initialize(attribute, attribute_id, klass)
        @attribute = attribute
        @attribute_id = attribute_id
        @class = klass
    end
end
