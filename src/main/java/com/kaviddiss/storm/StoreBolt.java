//package com.kaviddiss.storm;
//
//import backtype.storm.task.OutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.topology.base.BaseRichBolt;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Tuple;
//import backtype.storm.tuple.Values;
//import twitter4j.GeoLocation;
//import twitter4j.Status;
//
//import java.util.Arrays;
//import java.util.HashSet;
//import java.util.Map;
//import java.util.Set;
//
///**
// * Bolt filters out a predefined set of words.
// * @author davidk
// */
//public class StoreBolt extends BaseRichBolt {
//	
//	private static final long serialVersionUID = 6069146554651714103L;
//
//    private OutputCollector collector;
//
//    @Override
//    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
//        this.collector = collector;
//    }
//
//    @Override
//    public void execute(Tuple input) {
//    	Status tweet = (Status) input.getValueByField("tweet");
//    	String lat = (String) input.getValueByField("lat");
//    	String lon = (String) input.getValueByField("lon");
//    	String vote = (String) input.getValueByField("vote");
//        String username = tweet.getUser().getName();
//        String text = tweet.getText().toString();
//        String dateTime = tweet.getCreatedAt().toString();
//        
//		File file =new File("/Users/sonny/Documents/GeneralElection/"+vote+".txt");
//		Writer out =new FileWriter(file,true);
//		String data=vote.app("\r\n");
//		out.write(data);
//		out.close();
//
//    @Override
//    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("vote", "lat", "lon", "tweet"));
//    }
//}
