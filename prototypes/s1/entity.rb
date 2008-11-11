require 'stream.rb'
require 'objectmap.rb'
require 'attribute.rb'
require 'active_support/inflector'

class Entity
    def self.inherited(child)
        child.class_eval %q{
            @@primary_key = []
            @@all = ObjectMap.new { |key| self.new(key) }
            @@attributes = []
            @@has_many_assocs = []

            def self.primary_key
                @@primary_key
            end

            def self.primary_key=(key)
                @@primary_key = key
            end

            def self.all
                @@all
            end

            def self.attributes
                @@attributes
            end

            def self.has_many_assocs
                @@has_many_assocs
            end
        }


        def initialize(keys)
            keys.each_pair { |k, v| self.instance_variable_set("@#{k}", v) }
            @subscribers = []

            # Initialize has_many relation attributes
            self.class.has_many_assocs.each do |name|
                klass = eval(ActiveSupport::Inflector.singularize(name).capitalize)
                other_attr = self.class.to_s.downcase
                self.instance_variable_set("@#{name}", klass.all.having { |v| v.send(other_attr).cur() == self })
            end


            # Initialize all the attributes for this instance
            self.class.attributes.each do |attr|
                dependencies = attr[:dependencies].map { |d| d.to_s.split(".").inject(self) { |m, n| m = m.send(n) } }
                attribute = Attribute.new(self, dependencies, &attr[:block])
                self.instance_variable_set("@#{attr[:name]}", attribute)
                attribute.on_update do
                    @subscribers.each { |s| s.object_updated(self) }
                end
            end

            Clock.instance.on_advance do
                @subscribers.each { |s| s.object_updated(self) }
            end
        end


        def key
            Hash[*self.class.primary_key.zip(self.class.primary_key.map { |k| self.send(k) }).flatten]
        end


        def subscribe(listener)
            @subscribers << listener
        end
    end


    def self.derive_from(stream, options)
        self.primary_key = options[:unique_id]
        self.primary_key = [self.primary_key] if not self.primary_key.is_a? Enumerable

        constants, streams = stream.schema.fields.partition { |k| self.primary_key.include? k }

        define_method :initialize do |keys|
            @values = stream.filter do |t|
                pk = self.class.primary_key
                pk.map { |k| t.send(k) } == pk.map { |k| keys[k] }
            end

            super(keys)
        end

        # Define the attributes for this class
        streams.each do |s|
            attribute s, [:values] do |this|
                this.values.last[s]
            end
        end

        stream.schema.fields.each { |f| attr_reader f }
        attr_reader :values

        stream.subscribe(self)
    end


    def self.belongs_to(entity)
        klass = eval(entity.to_s.capitalize)
        entity_id = "#{entity}_id".to_sym

        attribute entity, [entity_id] do |this|
            klass.all[{ entity_id => this.send(entity_id).cur() }]
        end
    end


    def self.has_many(entity)
        klass = eval(ActiveSupport::Inflector.singularize(entity).capitalize)
        other_attr = self.to_s.downcase

        self.has_many_assocs << entity
        attr_reader entity
    end


    def self.attribute(name, dependencies, &block)
        self.attributes << { :name => name, :dependencies => dependencies, :block => block }
        attr_reader name
    end


    def self.add(tuple)
        key = Hash[*self.primary_key.zip(self.primary_key.map { |e| tuple[e]}).flatten]
        instance = self.all[key]
    end


    def self.all
        @@all
    end

    def pretty_print_instance_variables
        super - ["@subscribers", "@values"]
    end
end
