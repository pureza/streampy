require 'stream.rb'
require 'associations.rb'
require 'pp.rb'
require 'active_support/inflector'

$temperatures = Stream.new do |schema|
    schema.fields :sid, :temp, :room_id
end

$enter = Stream.new do |schema|
    schema.fields :pid, :room_id
end

$leave = Stream.new do |schema|
    schema.fields :pid, :room_id
end


class Entity
    def self.inherited(child)
        child.class_eval %q{
            @@primary_key = []
            @@all = Hash.new { |hash, keys| hash[keys] = self.new(keys) }
            @@associations = {
                :one_to_many => []
            }

            def self.primary_key
                @@primary_key
            end

            def self.primary_key=(key)
                @@primary_key = key
            end

            def self.all
                @@all
            end

            def self.associations
                @@associations
            end
        }

        def initialize(keys)
            keys.each_pair { |k, v| self.instance_variable_set("@#{k}", v) }
        end
    end

    def self.derive_from(stream, options)
        self.primary_key = options[:unique_id]
        self.primary_key = [self.primary_key] if not self.primary_key.is_a? Enumerable

        constants, streams = stream.schema.fields.partition { |k| self.primary_key.include? k }
        class_eval do
            define_method :initialize do |keys|
                super(keys)
                @values = stream.filter do |t|
                    pk = self.class.primary_key
                    pk.map { |k| t.send(k) } == pk.map { |k| keys[k] }
                end

                constants.each { |c| self.instance_variable_set("@#{c}", keys[c]) }
                streams.each { |s| self.instance_variable_set("@#{s}", @values.map { |t| { s => t[s] } }) }
            end
            stream.schema.fields.each { |f| attr_reader f }
        end

        stream.subscribe(self)
    end

    def self.belongs_to(entity)
        klass = eval(entity.to_s.capitalize)

        self.class_eval do
            attr_reader entity
            alias_method :old_init, :initialize
            define_method :initialize do |keys|
                old_init(keys)
                entity_id = "#{entity}_id".to_sym
                entity_id_stream = self.send(entity_id)
                self.instance_variable_set("@#{entity}", entity_id_stream.map { |t| { entity => klass.all[{entity_id => t.send(entity_id) }] } })
            end
        end
    end

    def self.has_many(entity)
        klass = eval(ActiveSupport::Inflector.singularize(entity).capitalize)
        other_attr = self.to_s.downcase

        self.class_eval do
            define_method entity do
#                klass.all.having { |v| v.send(other_attr).last[other_attr.to_sym] == self }.values
            end
        end
    end

    def self.defstream(name)
    end

    def self.add(tuple)
        key = Hash[*self.primary_key.zip(self.primary_key.map { |e| tuple[e]}).flatten]
        instance = self.all[key]
    end


    def self.all
        @@all
    end


    def to_s
        attributes = self.methods - Entity.methods
        str = attributes.map { |a| "  #{a} : #{self.send(a)}" }.join("\n")
        str
    end
end

class Sensor < Entity
end



class Room < Entity
    has_many :sensors
#    has_many :products

    defstream :temperature do
        @sensors.avg(:temp)
    end
end


class Sensor < Entity
    derive_from $temperatures, :unique_id => :sid
    belongs_to :room
end




class Product < Entity
    derive_from $enter, :unique_id => :pid

#    Room room =
#        | when enter(this.pid, rfid) -> rfid2room(rfid)
#        | when leave(this.pid, _) -> null

    defstream :temperature do
    # o defstream cria uma stream que subscreve a stream @room.temperature
        @room.temperature
    end

#    stop when leave(p.pid, rfid) where rfid2room(rfid) = room3
end



$temperatures.add Tuple.new(0, :sid => 1, :temp => 20, :room_id => 1)
$temperatures.add Tuple.new(5, :sid => 1, :temp => 30, :room_id => 1)
$temperatures.add Tuple.new(10, :sid => 2, :temp => 30, :room_id => 2)

pp Room.all[:room_id => 1].sensors

pp Sensor.all #.values.select { |s| s.room.room_id == 1 }
