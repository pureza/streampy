require 'clock.rb'

class ObjectMap
    attr_reader :objects

    def initialize(&init_block)
        @children = []
        @init_block = init_block
        @objects = {}
    end


    def [](key)
        if not @objects.has_key?(key) and @init_block
            self.add(key, @init_block.call(key))
        end
        @objects[key]
    end


    def add(key, object)
        @objects[key] = object
        @children.each { |c| c.add(key, object) }
    end


    def delete(object)
        @objects.delete(object.key)
        @children.each { |c| c.delete(object) }
    end


    def subscribe(child)
        @children << child
    end


    def having(&block)
        HavingObjectMap.new(self, &block)
    end

    def pretty_print_instance_variables
        [:objects]
    end

end


class HavingObjectMap < ObjectMap
    def initialize(parent, &block)
        super()
        @block = block
        parent.subscribe(self)
    end


    def add(key, object, subscribe=true)
        super(key, object) if @block.call(object)
        object.subscribe(self) if subscribe
    end


    def delete(object, unsubscribe=true)
        super(object)
        object.unsubscribe(self) if unsubscribe
    end


    def on_modified(object)
        if @block.call(object)
            add(object.key, object, false)
        else
            delete(object, false)
        end
    end


    def avg(field)
        map = self
        Class.new(Stream) do
            define_method :add do |key, object|
                objects = map.objects.values
                avg = objects.map { |o| o.send(field).last[field] }.inject(0) { |m, n| m += n } / objects.length
                super(Tuple.new(Clock.instance.now, field => avg))
            end
        end.new(self)
    end
end
