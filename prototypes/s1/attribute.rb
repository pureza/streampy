def interval(pair)
    pair.first
end


def value(pair)
    pair.last
end


class Attribute
    attr_reader :history

    def initialize(object, dependencies, &block)
        @object = object
        @history = []
        @update_actions = []
        @block = block
        dependencies.each do |d|
            d.on_update { self.reevaluate }
        end
        self.reevaluate
    end


    def reevaluate
        now = Clock.instance.now
        new_value = @block.call(@object)
        new_value = new_value.cur if new_value.is_a? Attribute

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


    def method_missing(name, *args)
         cur.send(name, *args)
    end


    def >(other)
        BooleanAttribute.new(self, other)
    end


    def pretty_print_instance_variables
        [:history]
    end
end


class BooleanAttribute < Attribute
    def initialize(parent, other)
        @history = parent.history.map { |interval, value| [interval, value > other] }
    end

    def any?
        @history.any? { |interval, value| value }
    end

    def always?
        @history.all? { |interval, value| value }
    end

    def for_more_than?(time)
        true_time = @history[0..-2].select { |i, v| v }.map { |i, v| i }.inject(0) { |acc, i| acc += (i.count - 1) }
        true_time += Clock.instance.now - @history.last[0].begin
        true_time > time
    end
end
