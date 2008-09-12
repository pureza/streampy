require 'tuple.rb'

class Stream

    attr_reader :data

    def initialize(parent = nil)
        @parent = parent
        @children = []
        @data = []
    end


    def subscribe(stream)
        @children << stream
        @data.each { |tuple| stream.add(tuple) }
    end


    def add(tuple)
        @data << tuple
        @children.each { |child| child.add(tuple) }
    end


    def remove(tuple)
        assert data.first == tuple
        @data.shift
        @children.each { |child| child.remove(tuple) }
    end


    def [](since)
        number, suffix = since.scan(/(\d+)\s*(\w*)/).first
        child = Stream.new(self)

        @data[-number..-1].each { |tuple| child.add(tuple) }
    end


    def filter(&block)
        child = Class.new(Stream) do
            define_method :add do |tuple|
                super(tuple) if block.call(tuple)
            end

            define_method :remove do |tuple|
                super(tuple) if block.call(tuple)
            end
        end.new(self)
        subscribe(child)
        child
    end


    def map(&block)
        child = Class.new(Stream) do
            define_method :add do |tuple|
                super(block.call(tuple))
            end

            define_method :remove do |tuple|
                super(block.call(tuple))
            end

        end.new(self)
        subscribe(child)
        child
    end


    def groupby(*fields, &block)
        child = Class.new do
            def initialize(parent)
                @substreams = Hash.new { |hash, key| hash[key] = Stream.new }
            end

            define_method :add do |tuple|
                key = fields.map { |field| tuple[field] }
                @substreams[key].add(tuple)
            end

            define_method :remove do |tuple|
                key = fields.map { |field| tuple[field] }
                @substreams[key].shift
            end

            define_method :[] do |key|
                block.call(@substreams[key])
            end
        end.new(self)
        subscribe(child)
        child
    end


    def fold(initial, function)
        @data.inject(initial) { |accum, tuple| accum = function.call(accum, tuple) }
    end


    def sum(field)
        fold(0, lambda { |m, n| m += n[field] })
    end


    def to_s
        @data.to_s
    end

end
