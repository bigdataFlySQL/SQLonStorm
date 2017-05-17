package bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.List;

/**
 * Created by yuxiao on 5/17/17.
 * having 语义过滤
 */
public class HavingBolt extends BaseBasicBolt {
    private List<String> descOfOutputFileds;

    public List<String> getDescOfOutputFileds() {
        return descOfOutputFileds;
    }

    public void setDescOfOutputFileds(List<String> descOfOutputFileds) {
        this.descOfOutputFileds = descOfOutputFileds;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        System.out.println(input.size());
        collector.emit(input.getValues());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields fields = new Fields(this.descOfOutputFileds);
        declarer.declare(fields);
    }
}
