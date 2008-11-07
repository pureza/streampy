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
