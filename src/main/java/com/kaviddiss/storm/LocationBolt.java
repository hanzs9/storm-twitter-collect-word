package com.kaviddiss.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.GeoLocation;
import twitter4j.Status;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Bolt filters out a predefined set of words.
 * @author davidk
 */
public class LocationBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 6069146554651714101L;

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
    	double latSW = 49.16;
    	double longSW = -10.65;
    	double latNE = 60.86;
    	double longNE = 1.6; // the bounding box of UK
    	double lat = 0; // initialize the location
    	double lon = 0;
    	
    	// get tweet and vote from the previous bolt
    	Status tweet = (Status) input.getValueByField("tweet");
    	String vote = (String) input.getValueByField("vote");
    	// get location information
    	GeoLocation location = tweet.getGeoLocation();
    	// if location is not null
    	if(location != null) {
    		double latitude = location.getLatitude();
    		double longitude = location.getLongitude();
    		System.out.println(tweet.getUser().getScreenName()+": "+tweet.getText().toString()+":: "+latitude+" "+longitude);
    		// if location is on the UK
    		if(latitude > latSW && latitude < latNE && longitude < longNE && longitude > longSW) {
    	    	System.out.println("UK"+tweet.getUser().getScreenName()+": "+tweet.getText().toString()+":: "+latitude+" "+longitude);
    			// sent this tweet to the next bolt
    	    	collector.emit(new Values(vote, lat, lon, tweet));
    		}
    	}
    	collector.emit(new Values(vote, lat, lon, tweet));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("vote", "lat", "lon", "tweet"));
    }
}
