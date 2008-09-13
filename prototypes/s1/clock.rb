require 'singleton.rb'

class Clock
    def initialize
        @@instance = self
    end

    def self.instance
        @@instance
    end
end

class SimClock < Clock
    attr_reader :now

    def initialize
        super
        @now = 0
        @advance_actions = []
    end

    def advance(step = 1)
        @now += step
        @advance_actions.each { |action| action.call() }
    end

    def on_advance(&action)
        @advance_actions << action
    end
end
