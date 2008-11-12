def interval(pair)
    pair.first
end


def value(pair)
    pair.last
end


# An Attribute is similar to a regular variable with one major difference:
# attributes can remember its past values.
#
# Also, attributes know their dependencies and recompute themselves
# automatically.
#
# The block that an attribute receives in the constructor is invoked to
# calculate the new value, on demand.
class Attribute
    attr_reader :history

    def initialize(name, object, dependencies, &block)
        @name = name
        @object = object
        @history = []
        @update_actions = []
        @block = block

        # Recompute the value when any dependency changes.
        dependencies.each do |d|
            d.on_update { self.reevaluate }
        end
        self.reevaluate
    end


    def reevaluate
        now = Clock.instance.now
        new_value = @block.call(@object)
        new_value = new_value.cur if new_value.is_a? Attribute

        # Add the new value to history, if it is different that the current one.
        if new_value != cur()
            @history.last[0] = ( interval(@history.last).begin .. now ) unless @history.empty?
            @history << [( now .. -1 ), new_value]
            @update_actions.each { |action| action.call(new_value) }
        end


    end


    def on_update(&block)
        @update_actions << block
    end


    def cur
        if @history.empty?
            nil
        else
            value(@history.last)
        end
    end


    # Dispatch unknown methods to the current object.
    # Allows, for instance, defining product.temperature as
    # product.room.temperature, with product.room being an attribute itself.
    def method_missing(name, *args)
         cur.send(name, *args)
    end


    [:>, :>=, :==, "!=", :<=, :<].each do |op|
        define_method op do |other|
            BooleanAttribute.new(self, other, op)
        end
    end


    def pretty_print_instance_variables
        [:history]
    end
end


class BooleanAttribute < Attribute

    def initialize(parent, other, operator)
        super("<bool>", parent, [parent]) do |ignore|
            parent.cur.send(operator, other)
        end
    end


    def any?
        Attribute.new("<any?>", self, [self]) do |bool_attr|
            bool_attr.history.any? { |i, v| v }
        end
    end


    def always?
        Attribute.new("<always?>", self, [self]) do |bool_attr|
            bool_attr.history.all? { |i, v| v }
        end
    end


    def during?(op, time, options={})
        attr = Attribute.new("<during?>", self, [self]) do |bool_attr|
            last = bool_attr.history.last
            if value(last)
                true_time = Clock.instance.now - interval(last).begin 
            else
                true_time = 0
            end

            if (options[:consecutive])
                p bool_attr.history
                p "consecutive not implemented"
            else
                intervals = bool_attr.history[0..-2].select { |i, v| v }.map { |i, v| i }
                true_time += intervals.inject(0) { |acc, i| acc += (i.count - 1) }
                true_time.send(op, time)
            end
        end
        # The during? attribute needs to be reevaluated as time advances.
        Clock.instance.on_advance { attr.reevaluate }
        attr
    end
end
