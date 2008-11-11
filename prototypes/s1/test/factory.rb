require 'entity.rb'
require 'pp.rb'

$temperatures = Stream.new do |schema|
    schema.fields :sid, :temp, :room_id
end

$enter = Stream.new do |schema|
    schema.fields :pid, :room_id
end

$leave = Stream.new do |schema|
    schema.fields :pid, :room_id
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
    derive_from $temperatures, :unique_id => :sid
    belongs_to :room
end


class Product < Entity
    derive_from $enter, :unique_id => :pid
    belongs_to :room

    attribute :temperature, [:room, "room.temperature"] do |product|
        product.room.temperature
    end
end


result = Product.all.having { |p| (p.temperature > 20).for_more_than?(50) }



$temperatures.add Tuple.new(Clock.instance.now, :sid => 1, :temp => 20, :room_id => 1)
Clock.instance.advance(5)
$enter.add Tuple.new(Clock.instance.now, :pid => 1001, :room_id => 1)

#blah = Product.all[:pid => 1001].temperature > 30

Clock.instance.advance(5)
$temperatures.add Tuple.new(Clock.instance.now, :sid => 1, :temp => 30, :room_id => 1)
Clock.instance.advance(5)
$temperatures.add Tuple.new(Clock.instance.now, :sid => 2, :temp => 40, :room_id => 2)
Clock.instance.advance(5)
$enter.add Tuple.new(Clock.instance.now, :pid => 1001, :room_id => 2)
Clock.instance.advance(60)
#pp blah

pp result

#pp "----------"
#pp Room.all[:room_id => 1]


#pp Sensor.all #.values.select { |s| s.room.room_id == 1 }



