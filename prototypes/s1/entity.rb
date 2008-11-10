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
        }


        def initialize(keys)
            keys.each_pair { |k, v| self.instance_variable_set("@#{k}", v) }
            @subscribers = []

            # Initialize all the attributes for this instance
            self.class.attributes.each do |attr|
                p attr
                attribute = Attribute.new(self, attr[:dependencies], attr[:block])
                self.instance_variable_set("@#{attr[:name]}", attribute)
                attribute.on_update do
                    @subscribers.each { |s| s.object_updated(self) }
                end
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
        class_eval do
            define_method :initialize do |keys|
                @values = stream.filter do |t|
                    pk = self.class.primary_key
                    pk.map { |k| t.send(k) } == pk.map { |k| keys[k] }
                end

                super(keys)
            end

            # Define the attributes for this class
            streams.each do |s|
                defstream s, [:values] do |this|
                    this.values.last[s]
                end
            end

            stream.schema.fields.each { |f| attr_reader f }
            attr_reader :values
        end

        stream.subscribe(self)
    end


    def self.belongs_to(entity)
        klass = eval(entity.to_s.capitalize)
        entity_id = "#{entity}_id".to_sym

        self.class_eval do
            defstream entity, [entity_id] do |this|
                klass.all[{ entity_id => this.send(entity_id).cur() }]
            end
        end
    end


    def self.has_many(entity)
        klass = eval(ActiveSupport::Inflector.singularize(entity).capitalize)
        other_attr = self.to_s.downcase

        self.class_eval do
            attr_reader entity
            alias_method :old_init2, :initialize
            define_method :initialize do |keys|
                p "ola"
                self.instance_variable_set("@#{entity}", klass.all.having { |v| v.send(other_attr).cur() == self })
                old_init2(keys)
            end
        end
    end


    def self.defstream(name, dependencies, &block)
        self.class_eval do
            self.attributes << { :name => name, :dependencies => dependencies, :block => block }
            attr_reader name
        end
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
