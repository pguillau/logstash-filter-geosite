# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# This example filter will replace the contents of the default 
# message field with whatever you specify in the configuration.
#
# It is only intended to be used as an example.
class LogStash::Filters::GeoSite < LogStash::Filters::Base

  # Setting the config_name here is required. This is how you
  # configure this filter from your Logstash config.
  #
  # filter {
  #   geosite {
  #     message => "My message..."
  #   }
  # }
  #
  config_name "geosite"
  
  # Replace the message with this value.
  config :message, :validate => :string, :default => "Welcome to GeoSite!"
  # the location of the gps file 
  config :database, :validate => :path, :required => true

  # the field from which we get the site's name
  config :source, :validate => :string, :required => true

  # the field in which we put the derived location
  config :target, :validate => :string, :default => 'geoip'

  # hash of locations
  config :locs 

  public
  def register
    # Add instance variables 
    if @database.nil?
      if !File.exists?(@database)
        raise "You must specify 'database => ...' in your dcCabiGeo filter (I looked for '#{@database}'"
      end
    end
    
    @locs = Hash.new
    CSV.foreach(@database, { :col_sep => ';' }) do |row|
	  
      @locs[row[0]] = [ row[2].to_f, row[3].to_f ]
	  #puts @locs[row[0]]
    end
  end # def register

  public
  
  def filter(event)
    
    geo_data = nil
	ip = event[@source]
    ip = ip.first if ip.is_a? Array
	event[@target] = {} if event[@target].nil?
	if @locs[ip]
        geo_data = @locs[ip]
		event[@target][:location] = [ geo_data[1].to_f, geo_data[0].to_f ]
    end
    
    filter_matched(event)
  end # def filter
  
end # class LogStash::Filters::GeoSite