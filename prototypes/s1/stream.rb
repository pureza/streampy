require 'tuple.rb'
require 'clock.rb'

class Stream

    attr_reader :data

    def initialize(parent = nil)
        @parent = parent
        @children = []
        @data = []
        parent.subscribe(self) if parent
    end


    def subscribe(stream)
        @children << stream
        data.each { |tuple| stream.add(tuple) }
    end


    def add(tuple)
        data << tuple
        @children.each { |child| child.add(tuple) }
    end


    def remove(tuple)
        raise unless tuple == data.first
        data.shift
        @children.each { |child| child.remove(tuple) }
    end


    def [](since)
        WindowedStream.new(self, since)
    end


    def last
        data.last
    end


    def filter(&block)
        Class.new(Stream) do
            define_method :add do |tuple|
                super(tuple) if block.call(tuple)
            end

            define_method :remove do |tuple|
                super(tuple) if block.call(tuple)
            end
        end.new(self)
    end


    def map(&block)
        Class.new(Stream) do
            define_method :add do |tuple|
                new_tuple = block.call(tuple)
                super(block.call(tuple))
            end

            define_method :remove do |tuple|
                super(block.call(tuple))
            end

        end.new(self)
    end


    def groupby(*fields, &block)
        child = Class.new do
            def initialize(parent)
                @substreams = Hash.new { |hash, key| hash[key] = Stream.new }
            end

            define_method :add do |tuple|
                key = fields.map { |field| tuple[field] }
                key = key[0] if key.length == 1
                @substreams[key].add(tuple)
            end

            define_method :remove do |tuple|
                key = fields.map { |field| tuple[field] }
                key = key[0] if key.length == 1
                @substreams[key].shift
            end

            define_method :[] do |key|
                block.call(@substreams[key])
            end
        end.new(self)
        subscribe(child)
        child
    end


    def partitionby(*fields, &block)
        child = Class.new(Stream) do
            def initialize(parent)
                @substreams = Hash.new { |hash, key| hash[key] = Stream.new }
                super
            end

            define_method :add do |tuple|
                key = fields.map { |field| tuple[field] }
                key = key[0] if key.length == 1
                @substreams[key].add(tuple)
            end

            define_method :remove do |tuple|
                key = fields.map { |field| tuple[field] }
                key = key[0] if key.length == 1
                @substreams[key].shift
            end

            define_method :data do
                @substreams.values.map { |stream| block.call(stream) }.flatten.sort { |a, b| a.timestamp <=> b.timestamp }
            end
        end.new(self)
        subscribe(child)
        child
    end


    def fold(initial, function)
        data.inject(initial) { |accum, tuple| accum = function.call(accum, tuple) }
    end


    def sum(field)
        fold(0, lambda { |m, n| m += n[field] })
    end


    def avg(field)
        sum(field) / data.length
    end


    def to_s
        data.to_s
    end
end


class WindowedStream < Stream
    def initialize(parent, window)
        @clock = Clock.instance
        @clock.on_advance do
            while data.first.timestamp < @clock.now - @window
                remove(data.first)
            end
        end
        @window = window
        super(parent)
    end


    def add(tuple)
        super if tuple.timestamp >= @clock.now - @window
    end
end


class Fixnum
    def s
        self
    end

    def min
        self * 60
    end
end
