class Stream

    attr_reader :data

    def initialize(parent = nil)
        @parent = parent
        @children = []
        @data = []
        parent.data.each { |tuple| add(tuple) } if parent
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
        child = Stream.new(self) do
            alias :old_add :add
            alias :old_remove :remove

            def add(tuple)
                old_add(tuple) if block(tuple)
            end

            def remove(tuple)
                old_remove(tuple) if block(tuple)
            end
        end
        @children << child
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
        @children << child
        child
    end


    def groupby(*fields, &block)
        Class.new do
            def initialize
                @parent.children << self
                @substreams = {}
            end

            def add(tuple)
                key = fields.map { |field| new.send(field) }
                substream[key] << new
            end

            def remove(tuple)
                key = fields.map { |field| new.send(field) }
                substream[key].shift
            end

            def [](key)
                block(substream[key])
            end
        end
    end


    def fold(initial, &block)
        @data.inject(initial) { |accum, tuple| block(accum, tuple) }
    end


    def sum(field)
        fold(0, lambda { |m, n| m += n })
    end


    def to_s
        @data.to_s
    end

end
