# encoding: utf-8
require_relative '../spec_helper'
require "logstash/filters/flatten_json"
require 'json'

describe LogStash::Filters::FlattenJson do
  describe "Flatten json" do
    let(:config) do <<-CONFIG
      filter {
        flatten_json {
          source => "message"
        }
      }
    CONFIG
    end

    sample({"message" => {"key_1" => "string",
                          "key_2" => 1,
                          "key_3" => {
                              "nested_key_1" => "string",
                              "nested_key_2" => [1, "a"]}
                         }}) do

      expect(subject.get('key_1')).to eq('string')
      expect(subject.get('key_2')).to eq(1)
      expect(subject.get('key_3.nested_key_1')).to eq('string')
      expect(subject.get('key_3.nested_key_2.0')).to eq(1)
      expect(subject.get('key_3.nested_key_2.1')).to eq('a')
    end
  end
end
