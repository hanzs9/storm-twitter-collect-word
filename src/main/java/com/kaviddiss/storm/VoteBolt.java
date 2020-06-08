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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Bolt filters out a predefined set of words.
 * @author davidk
 */
public class VoteBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 6069146554651714100L;

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
    	// receive the tweet from the previous bolt
    	Status tweet = (Status) input.getValueByField("tweet");
    	// get the text and convert to lower case
    	String text = tweet.getText().toString().toLowerCase();
    	// if the text contains conservative
        if (text.contains("conservative")) {
        	// sent this tweet to next bolt
            collector.emit(new Values("conservative",tweet));
            storeVote("conservative");
            storeInf(tweet,"conservative");
        // if text contains labour
        } else if(text.contains("labour")) {
        	// sent this tweet to next bolt
        	collector.emit(new Values("labour",tweet));
        	storeVote("labour");
        	storeInf(tweet, "labour");
        // if text contains liberal democrat
        } else if(text.contains("liberal democrat")) {
        	// sent this tweet to next bolt
        	collector.emit(new Values("liberal democrat",tweet));
        	storeVote("liberal democrat");
        	storeInf(tweet, "liberal democrat");
        // if text contains green party
        } else if(text.contains("green party")) {
        	// sent this tweet to next bolt
        	collector.emit(new Values("green party",tweet));
        	storeVote("green party");
        	storeInf(tweet, "green party");
        // if text contains green party
        } else if(text.contains("brexit party")) {
        	collector.emit(new Values("brexit party",tweet));
        	storeVote("brexit party");
        	storeInf(tweet, "brexit party");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("vote", "tweet"));
    }
    
    public void storeVote(String vote) {
		try {
			File file =new File("/Users/sonny/Documents/GeneralElection/"+vote+".txt");
			Writer out = new FileWriter(file,true);
			StringBuilder builder = new StringBuilder();
			builder.append(vote);
			builder.append("\r\n");
			out.write(builder.toString());
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public void storeInf(Status tweet, String vote) {
		try {
			File file =new File("/Users/sonny/Documents/GeneralElection/"+vote+"Content.txt");
			Writer out = new FileWriter(file,true);
			StringBuilder builder = new StringBuilder();
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
