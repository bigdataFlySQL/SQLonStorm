package topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import spouts.StreamDataReaderSpout;


import bolts.SelectBolt;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
         
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("data-reader",new StreamDataReaderSpout());
		builder.setBolt("word-normalizer", new SelectBolt())
			.shuffleGrouping("data-reader");
//		builder.setBolt("word-counter", new WordCounter(),2)
//				.shuffleGrouping("word-normalizer");
//		builder.setBolt("word-counter", new WordCounter(),2)
//			.fieldsGrouping("word-normalizer", new Fields("word"));
		
        //Configuration
		Config conf = new Config();
		conf.put("InputSQL", args[0]);
		conf.setDebug(false);
        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
		Thread.sleep(1000);
		cluster.shutdown();
	}
}
