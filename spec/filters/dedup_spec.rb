require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/dedup"

describe LogStash::Filters::Dedup do

  context "with basic meter config" do
    context "when no events were received" do
      it "should not flush" do
        config = {"key" => ["pk"]}
        filter = LogStash::Filters::Dedup.new config
        filter.register

        events = filter.flush
        insist { events }.nil?
      end
    end

    context "when events are received" do
      context "on the first flush" do
        subject {
          config = {"key" => ["pk"]}
          filter = LogStash::Filters::Dedup.new config
          filter.register
          filter.filter LogStash::Event.new({"pk" => "de_region_4711", "amount" => 2000})
          filter.filter LogStash::Event.new({"pk" => "de_region_4711", "amount" => 1000})
          filter.filter LogStash::Event.new({"pk" => "de_region_4711", "amount" => 3000})
          filter.filter LogStash::Event.new({"pk" => "fr_region_4711", "amount" => 7000})

          filter.flush
        }

        it "should return the lastest version of each key" do
          insist { subject.length } == 1
          insist { subject.first.get("de_region_4711")["amount"] } == 3000
          insist { subject.first.get("fr_region_4711")["amount"] } == 7000
        end       
      end    
    end   
  end
end
