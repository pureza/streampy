require 'entity.rb'
require 'pp.rb'

$temperatures = Stream.new do |schema|
    schema.fields :sensor_id, :temp, :room_id
end

$enter = Stream.new do |schema|
    schema.fields :product_id, :room_id
end

$leave = Stream.new do |schema|
    schema.fields :product_id, :room_id
end

SimClock.new


class Sensor < Entity
end

class Product < Entity
end

class Room < Entity
    has_many :sensors
    has_many :products

    attribute :temperature, [:sensors] do |room|
        room.sensors.avg(:temp)
    end
end


class Sensor < Entity
    derive_from $temperatures, :unique_id => :sensor_id
    belongs_to :room
end


class Product < Entity
    derive_from $enter, :unique_id => :product_id
    belongs_to :room

    attribute :temperature, [:room, "room.temperature"] do |product|
        product.room.temperature
    end
end


result = Product.all.having { |p| (p.temperature >= 20).during?(:>=, 10, :consecutive => false) }


$temperatures.add Tuple.new(Clock.instance.now, :sensor_id => 1, :temp => 20, :room_id => 1)
Clock.instance.advance(5)

$enter.add Tuple.new(Clock.instance.now, :product_id => 1001, :room_id => 1)
Clock.instance.advance(5)

$temperatures.add Tuple.new(Clock.instance.now, :sensor_id => 1, :temp => 30, :room_id => 1)
Clock.instance.advance(5)

$temperatures.add Tuple.new(Clock.instance.now, :sensor_id => 2, :temp => 40, :room_id => 2)
Clock.instance.advance(5)

$enter.add Tuple.new(Clock.instance.now, :product_id => 1001, :room_id => 2)
#Clock.instance.advance(60)


pp result



