package com.kaviddiss.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * Topology class that sets up the Storm topology for this sample.
 * Please note that Twitter credentials have to be provided as VM args, otherwise you'll get an Unauthorized error.
 * @link http://twitter4j.org/en/configuration.html#systempropertyconfiguration
 */
public class Topology {

	static final String TOPOLOGY_NAME = "storm-twitter-word-count";

	public static void main(String[] args) {
		// build a new configuration
		Config config = new Config();
		// set the time out to stop
		config.setMessageTimeoutSecs(120);

		// set up a new storm topology
		TopologyBuilder b = new TopologyBuilder();
		// new a twitter sample spout
		b.setSpout("TwitterSampleSpout", new TwitterSampleSpout());
		// the tuples from spout are sent to the hashtag bolt with shuffle grouping
        b.setBolt("HashtagBolt", new HashtagBolt()).shuffleGrouping("TwitterSampleSpout");
        // filtered tuples are sent to the next bolt called vote bolt
        b.setBolt("VoteBolt", new VoteBolt()).shuffleGrouping("HashtagBolt");
        // tuples are sent to location bolt to check location
		b.setBolt("LocationBolt", new HashtagBolt()).shuffleGrouping("TwitterSampleSpout");
		// tuples are sent to word counter bolt to count the number of tweets
        b.setBolt("WordCounterBolt", new WordCounterBolt(10, 30 * 60, 50)).shuffleGrouping("VoteBolt");
        
        // use storm local mode
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.killTopology(TOPOLOGY_NAME);
				cluster.shutdown();
			}
		});

	}

}
