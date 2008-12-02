require 'clock.rb'

class ObjectMap
    attr_reader :objects

    def initialize(&init_block)
        @children = []
        @update_actions = []
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
        notify
    end


    def delete(object)
        @objects.delete(object.key)
        @children.each { |c| c.delete(object) }
        notify
    end


    def subscribe(child)
        @children << child
        @objects.each { |k, v| child.add(k, v) }
    end


    def on_update(&block)
        @update_actions << block
    end


    def notify
        @update_actions.each { |action| action.call }
    end


    def having(&block)
        HavingObjectMap.new(self, &block)
    end

    def pretty_print_instance_variables
        [:objects]
    end
end


# A HavingObjectMap represents a view of a standard ObjectMap filtered by some
# condition.
#
# HavingObjectMaps have essentially two responsabilities:
# - Monitor dynamically which objects pass the filter (i.e., as the objects
#   themselves change, they may enter or leave the view).
#   This is done with the help of Attributes. I.e, for each object there
#   is a boolean attribute that monitors the object and, if it evaluates
#   to true, the object is considered part of the view and the opposite
#   otherwise.
#
# - Notify external observers if any object currently in the view changes.
#   This is necessary, for instance, in the case where the room's temperature
#   is defined as the average temperature of all the sensors in the room.
#   We want to update the room's temperature not only as new sensors are added
#   or removed from the room, but also as they update their own readings.
#
# Note: The result of @block.call(...) must be a new attribute that monitors
#       each object!
class HavingObjectMap < ObjectMap

    alias_method :parent_add, :add
    alias_method :parent_delete, :delete

    attr_reader :monitor

    def initialize(parent, &block)
        super()
        @monitor = {}
        @block = block
        parent.subscribe(self)
    end


    # Called when the parent added some object.
    # Since this is a filtered view from the parent, we mustn't add the object
    # blindly, instead, we should monitor it.
    def add(key, object)
        if not @monitor.has_key?(key)
            @monitor[key] = @block.call(object)
            @monitor[key].on_update { |value| collection_updated(object, value) }
            collection_updated(object, @monitor[key].cur) # FIXME: First notification should be automatic
        end
    end


    # Called when the parent deleted some object.
    # Since this is a filtered view from the parent, we must delete the object
    # too.
    def delete(object)
        @monitor.delete(object.key)
        object.unsubscribe(self)
        super(object)
    end


    # When an attribute changes, check if its corresponding object is now part
    # of the view, or it's not anymore.
    # <result> is the current value of the attribute.
    def collection_updated(object, result)
        key = object.key
        if result
            parent_add(key, object)
            object.subscribe(self)
        else
            object.unsubscribe(self)
            parent_delete(object)
        end
    end


    # Some object has been modified. Notify the subscribers.
    def object_updated(object)
        notify
    end


    def avg(field)
        if @objects.empty?
            nil
        else
            @objects.values.map { |o| o.send(field).cur() }.inject(0) { |m, n| m += n } / @objects.length
        end
    end


    def pretty_print_instance_variables
        super + [:monitor]
    end
end
