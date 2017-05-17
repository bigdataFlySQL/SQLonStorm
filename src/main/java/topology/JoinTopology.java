package topology;

import bolts.JoinBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.utils.Utils;
import spouts.JoinSpouttest_1;
import spouts.JoinSpouttest_2;

import java.util.concurrent.TimeUnit;

/**
 *
 * Created by yao on 5/16/17.
 */
public class JoinTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("data_reader_1",new JoinSpouttest_1());
        builder.setSpout("data_reader_2",new JoinSpouttest_2());
        BaseWindowedBolt bolt = new JoinBolt().withTumblingWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS));
        builder.setBolt("join_bolt",bolt,1).shuffleGrouping("data_reader_1").shuffleGrouping("data_reader_2");
        Config conf = new Config();
        conf.put("InputSQL", args[0]);
        conf.setDebug(false);

        //Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
        Thread.sleep(8000);
        cluster.shutdown();
    }
}
