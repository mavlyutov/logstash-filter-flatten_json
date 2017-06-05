# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# This  filter will replace the contents of the default 
# message field with whatever you specify in the configuration.
#
# It is only intended to be used as an .
class LogStash::Filters::FlattenJson < LogStash::Filters::Base

  config_name "flatten_json"

  # The configuration for the filter:
  # [source,ruby]
  #     source => source_field
  #
  # The above would parse the json from the `message` field
  config :source, :validate => :string, :required => true

  public
  def register
    # Nothing to do here
  end

  public
  def filter(event)
    @logger.debug? && @logger.debug("Running flatten_json filter", :event => event)

    source = event.get(@source)
    return unless source

    flatten_json = flatten(source, '')
    flatten_json.each do |key, value|
      event.set(key, value)
    end

    # filter_matched should go in the last line of our successful code
    filter_matched(event)
    @logger.debug? && @logger.debug("Event after flatten_json filter", :event => event)
  end

  def flatten(json, prefix)
    if json && !json.empty?
      json.keys.each do |key|
        if prefix.empty?
          full_path = key
        else
          full_path = [prefix, key].join('.')
        end

        if json[key].is_a?(Hash)
          value = json[key]
          json.delete key
          json.merge! flatten(value, full_path)
        elsif json[key].is_a?(Array)
          json[key].each_with_index do |item, index|
            current_path = [full_path, index].join('.')
            if item.is_a?(Hash)
              json.merge! flatten(item, current_path)
            else
              json[current_path] = item
            end
          end
          json.delete key
        else
          value = json[key]
          json.delete key
          json[full_path] = value
        end
      end
    end
    return json
  end

end
