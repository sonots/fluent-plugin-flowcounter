require 'fluent/mixin/config_placeholders'

class Fluent::FlowCounterOutput < Fluent::Output
  Fluent::Plugin.register_output('flowcounter', self)

  config_param :unit, :string, :default => 'minute'
  config_param :aggregate, :string, :default => 'tag'
  config_param :output_style, :string, :default => 'joined'
  config_param :tag, :string, :default => 'flowcount'
  config_param :input_tag_remove_prefix, :string, :default => nil
  config_param :count_keys, :string
  config_param :enable_count, :bool, :default => true
  config_param :enable_bytes, :bool, :default => true
  config_param :enable_rate, :bool, :default => true

  include Fluent::Mixin::ConfigPlaceholders

  attr_accessor :counts
  attr_accessor :last_checked
  attr_accessor :count_all
  attr_reader :tick

  def configure(conf)
    super

    @unit = case @unit
            when 'second' then :second
            when 'minute' then :minute
            when 'hour' then :hour
            when 'day' then :day
            else
              raise Fluent::ConfigError, "flowcounter unit allows second/minute/hour/day"
            end
    @tick = case @unit
            when :second then 1
            when :minute then 60
            when :hour then 3600
            when :day then 86400
            else
              raise Fluent::ConfigError, "flowcounter unit allows second/minute/hour/day"
            end
    @aggregate = case @aggregate
                 when 'tag' then :tag
                 when 'all' then :all
                 else
                   raise Fluent::ConfigError, "flowcounter aggregate allows tag/all"
                 end
    @output_style = case @output_style
                 when 'joined' then :joined
                 when 'tagged' then :tagged
                 else
                   raise Fluent::ConfigError, "flowcounter output_style allows joined/tagged"
                 end
    if @output_style == :tagged and @aggregate != :tag
      raise Fluent::ConfigError, "flowcounter aggregate must be 'tag' when output_style is 'tagged'"
    end
    if @input_tag_remove_prefix
      @removed_prefix_string = @input_tag_remove_prefix + '.'
      @removed_length = @removed_prefix_string.length
    end
    @count_keys = @count_keys.split(',')
    @count_all = (@count_keys == ['*'])

    @count_proc = count_proc_new
    @counts = count_initialized
    @mutex = Mutex.new
  end

  def start
    super
    start_watch
  end

  def shutdown
    super
    @watcher.terminate
    @watcher.join
  end

  def count_initialized(keys=nil)
    if @aggregate == :all
      {'count' => 0, 'bytes' => 0}
    elsif keys
      values = Array.new(keys.length){|i| 0 }
      Hash[[keys, values].transpose]
    else
      {}
    end
  end

  def countup(name, counts, bytes)
    c = 'count'
    b = 'bytes'
    if @aggregate == :tag
      c = name + '_count'
      b = name + '_bytes'
    end
    @mutex.synchronize {
      @counts[c] = (@counts[c] || 0) + counts if @enable_count
      @counts[b] = (@counts[b] || 0) + bytes  if @enable_bytes
    }
  end

  def generate_output(counts, step)
    rates = {}
    counts.keys.each {|key|
      rates[key + '_rate'] = ((counts[key] * 100.0) / (1.00 * step)).floor / 100.0
    } if @enable_rate
    counts.update(rates)
  end

  def flush(step)
    flushed,@counts = @counts,count_initialized(@counts.keys)
    generate_output(flushed, step)
  end

  def tagged_flush(step)
    flushed,@counts = @counts,count_initialized(@counts.keys)
    names =
      if @enable_count
        flushed.keys.select {|x| x.end_with?('_count')}.map {|x| x.chomp('_count')}
      elsif @enable_bytes
        flushed.keys.select {|x| x.end_with?('_bytes')}.map {|x| x.chomp('_bytes')}
      end
    names.map {|name|
      counts = {}
      counts['count'] = flushed[name + '_count'] if @enable_count
      counts['bytes'] = flushed[name + '_bytes'] if @enable_bytes
      data = generate_output(counts, step)
      data['tag'] = name
      data
    }
  end

  def flush_emit(step)
    if @output_style == :tagged
      tagged_flush(step).each do |data|
        Fluent::Engine.emit(@tag, Fluent::Engine.now, data)
      end
    else
      Fluent::Engine.emit(@tag, Fluent::Engine.now, flush(step))
    end
  end

  def start_watch
    # for internal, or tests only
    @watcher = Thread.new(&method(:watch))
  end

  def watch
    # instance variable, and public accessable, for test
    @last_checked = Fluent::Engine.now
    while true
      sleep 0.5
      if Fluent::Engine.now - @last_checked >= @tick
        now = Fluent::Engine.now
        flush_emit(now - @last_checked)
        @last_checked = now
      end
    end
  end

  def count_proc_new
    if @count_all
      if @enable_count and @enable_bytes
        Proc.new {|es|
          count, bytes = 0, 0
          es.each {|time,record|
            count += 1
            bytes += record.to_msgpack.bytesize
          }
          [count, bytes]
        }
      elsif @enable_count
        Proc.new {|es|
          count, bytes = 0, 0
          es.each {|time,record|
            count += 1
          }
          [count, bytes]
        }
      elsif @enable_bytes
        Proc.new {|es|
          count, bytes = 0, 0
          es.each {|time,record|
            bytes += record.to_msgpack.bytesize
          }
          [count, bytes]
        }
      else
        Proc.new {|es| [0, 0] }
      end
    elsif @count_keys
      if @enable_count and @enable_bytes
        Proc.new {|es|
          count, bytes = 0, 0
          es.each {|time,record|
            count += 1
            bytes += @count_keys.map {|k| record[k].bytesize }.inject(:+)
          }
          [count, bytes]
        }
      elsif @enable_count
        Proc.new {|es|
          count, bytes = 0, 0
          es.each {|time,record|
            count += 1
          }
          [count, bytes]
        }
      elsif @enable_bytes
        Proc.new {|es|
          count, bytes = 0, 0
          es.each {|time,record|
            bytes += @count_keys.map {|k| record[k].bytesize }.inject(:+)
          }
          [count, bytes]
        }
      else
        Proc.new {|es| [0,0] }
      end
    end
  end

  def emit(tag, es, chain)
    name = tag
    if @input_tag_remove_prefix and
        ( (tag.start_with?(@removed_prefix_string) and tag.length > @removed_length) or tag == @input_tag_remove_prefix)
      name = tag[@removed_length..-1]
    end
    count, bytes = @count_proc.call(es)
    countup(name, count, bytes)

    chain.next
  end
end
