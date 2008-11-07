require 'stream.rb'
require 'objectmap.rb'
require 'active_support/inflector'

class Entity
    def self.inherited(child)
        child.class_eval %q{
            @@primary_key = []
            @@all = ObjectMap.new(self)

            def self.primary_key
                @@primary_key
            end

            def self.primary_key=(key)
                @@primary_key = key
            end

            def self.all
                @@all
            end
        }

        def initialize(keys)
            keys.each_pair { |k, v| self.instance_variable_set("@#{k}", v) }
        end


        def id
            @@primary_key.map { |k| self.send(k) }
        end
    end

    def self.derive_from(stream, options)
        self.primary_key = options[:unique_id]
        self.primary_key = [self.primary_key] if not self.primary_key.is_a? Enumerable

        constants, streams = stream.schema.fields.partition { |k| self.primary_key.include? k }
        class_eval do
            define_method :initialize do |keys|
                super(keys)
                @values = stream.filter do |t|
                    pk = self.class.primary_key
                    pk.map { |k| t.send(k) } == pk.map { |k| keys[k] }
                end

                constants.each { |c| self.instance_variable_set("@#{c}", keys[c]) }
                streams.each { |s| self.instance_variable_set("@#{s}", @values.map { |t| { s => t[s] } }) }
            end
            stream.schema.fields.each { |f| attr_reader f }
        end

        stream.subscribe(self)
    end

    def self.belongs_to(entity)
        klass = eval(entity.to_s.capitalize)

        self.class_eval do
            attr_reader entity
            alias_method :old_init, :initialize
            define_method :initialize do |keys|
                old_init(keys)
                entity_id = "#{entity}_id".to_sym
                entity_id_stream = self.send(entity_id)
                self.instance_variable_set("@#{entity}", entity_id_stream.map { |t| { entity => klass.all[{entity_id => t.send(entity_id) }] } })
            end
        end
    end

    def self.has_many(entity)
        klass = eval(ActiveSupport::Inflector.singularize(entity).capitalize)
        other_attr = self.to_s.downcase

        self.class_eval do
            attr_reader entity
            alias_method :old_init, :initialize
            define_method :initialize do |keys|
                old_init(keys)
                self.instance_variable_set("@#{entity}", klass.all.having { |v| v.send(other_attr).last[other_attr.to_sym] == self })
            end
        end
    end

    def self.defstream(name)
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
