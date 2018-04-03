# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# TODO add documentation
class LogStash::Filters::Dedup < LogStash::Filters::Base
  config_name "dedup"

  # The flush interval, when the dedup event is created. Must be a multiple of 5s.
  config :flush_interval, :validate => :number, :default => 5

  # TODO add documentation
  config :key, :validate => :string, :required => true

  
  def register
    require "atomic"
    require "concurrent"
    @last_flush = Atomic.new(0) # seconds since last flush
    @container = Concurrent::Map.new
  end # def register

  def filter(event)
    key = event.get(@key)
    
    if @container[key] == nil
      @container[key] = event.clone
    else
      @container[key] = event.clone if event.timestamp >= @container[key].timestamp
    end
    
    event.cancel
  end # def filter

  def flush(options = {})
    # Add 5 seconds to @last_flush counter
    # since this method is called every 5 seconds.
    @last_flush.update { |v| v + 5 }
    return unless should_flush?
     
    result = []
    @container.each_pair do |key, event|
      filter_matched(event)       
      result << event        
    end

    @last_flush.value = 0
    @container.clear
    result
  end

  # Workaround for
  # https://github.com/elasticsearch/logstash/issues/1839
  def periodic_flush
    true
  end

  private
  def should_flush?
    @last_flush.value >= @flush_interval && !@container.empty?
  end

end # class LogStash::Filters::Dedup
