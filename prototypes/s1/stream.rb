require 'tuple.rb'
require 'clock.rb'

class Stream

    attr_reader :data, :children

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
        Class.new(Stream) do
            define_method :add do |tuple|
            end

            define_method :remove do |tuple|
            end

            define_method :data do
                @parent.data.last
            end
        end.new(self)
    end


    def filter(&predicate)
        Class.new(Stream) do
            define_method :add do |tuple|
                super(tuple) if predicate.call(tuple)
            end

            define_method :remove do |tuple|
                super(tuple) if predicate.call(tuple)
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
                @substreams.values.map { |stream| block.call(stream).data }.flatten.sort { |a, b| a.timestamp <=> b.timestamp }
            end
        end.new(self)
    end


    def fold(initial, function)
        Class.new(Stream) do
            define_method :add do |tuple|
            end

            define_method :remove do |tuple|
            end

            define_method :data do
                @parent.data.inject(initial) { |accum, tuple| accum = function.call(accum, tuple) }
            end
        end.new(self)
    end


    def sum(field)
        fold(0, lambda { |m, n| m += n[field] })
    end


    def avg(field)
        Class.new(Stream) do
            define_method :add do |tuple|
            end

            define_method :remove do |tuple|
            end

            define_method :data do
                @parent.sum(field).data / @parent.data.length
            end
        end.new(self)
    end


    def min(field)
        fold(data.first[field], lambda { |m, n| m = [m, n[field]].min })
    end

    def >>(action)
        action.call(self)
    end


    def to_s
        data.to_s
    end
end


def groupby(*fields, &block)
    lambda { |stream| stream.groupby(*fields, &block) }
end


def partitionby(*fields, &block)
    lambda { |stream| stream.partitionby(*fields, &block) }
end


def map(&block)
    lambda { |stream| stream.map(&block) }
end


def filter(&predicate)
    lambda { |stream| stream.filter(&predicate) }
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
