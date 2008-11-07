class ObjectMap
    attr_reader :elements_type

    def initialize(elements_type)
        @children = []
        @elements_type = elements_type
        @objects = Hash.new do |hash, key|
            hash[key] = elements_type.new(key)
            @children.each { |child| child.on_add(key, hash[key]) }
        end
    end


    def [](key)
        @objects[key]
    end


    def subscribe(child)
        @children << child
    end


    def having(&block)
        HavingObjectMap.new(self, &block)
    end
end


class HavingObjectMap
    def initialize(parent, &block)
        @objects = {}
        @block = block
        parent.subscribe(self)
    end


    def [](key)
        @objects[key]
    end


    def add(key, obj)
        @objects[key] = obj
        obj.subscribe(self)
    end


    def delete(key)
        @objects[key].unsubscribe(self)
        @objects.delete(key)
    end


    def on_modified(obj)
        on_add(obj.id, obj)
    end


    def on_add(key, obj)
        begin
            if @block.call(obj)
                self.add(key, obj)
            else
                self.delete(key)
            end
        rescue
        end
    end
end
