package com.kaviddiss.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Date;
import java.util.Map;

/**
 * Receives tweets and emits its words over a certain length.
 */
public class HashtagBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 5151173513759399636L;

    private OutputCollector collector;
    
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Status tweet = (Status) input.getValueByField("tweet");
        // get user name
        String username = tweet.getUser().getName();
        // get tweet content
        String text = tweet.getText().toString();
        // get tweet create time
        String dateTime = tweet.getCreatedAt().toString();
        // get tweet location
        GeoLocation location = tweet.getGeoLocation();
        
        // check whether it has hash tag or not
        for(HashtagEntity hashtag : tweet.getHashtagEntities()) {
        	// if hashtag contains word about election
        	if(hashtag.getText().contains("GeneralElection")||hashtag.getText().contains("GE2019")) {
        		System.out.println(hashtag.getText()+": "+username+": "+text+" Date: "+dateTime);
        		// this tweet will be sent to the next bolt 
        		collector.emit(new Values(tweet));
        		// the tweet will be store in local file
        		storeInf(tweet);
        	}
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
    
    public void storeInf(Status tweet) {
		try {
			// save tweet in the local file 
			File file =new File("/Users/sonny/Documents/GeneralElection/content.txt");
			Writer out = new FileWriter(file,true);
			// save with string format
			StringBuilder builder = new StringBuilder();
			// save date, username, content
			builder.append(" Date: "+tweet.getCreatedAt().toString());
			builder.append(" username: "+tweet.getUser().getName());
			builder.append(" content: "+tweet.getText().toString());
			if(tweet.getGeoLocation() != null) {
				builder.append(" loc: "+tweet.getGeoLocation().getLatitude()+" "+tweet.getGeoLocation().getLongitude());
			}
			builder.append("\r\n");
			out.write(builder.toString());
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
