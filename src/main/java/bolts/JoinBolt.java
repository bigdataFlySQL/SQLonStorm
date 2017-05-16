package bolts;

import jdk.nashorn.internal.scripts.JO;
import operation.JoinCondition;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.thrift.TUnion;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 *
 * Created by yao on 5/16/17.
 */
public class JoinBolt extends BaseWindowedBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BaseWindowedBolt.class);

    List<Tuple> List_joinTab = new ArrayList<Tuple>();
    List<Tuple> List_originTab = new ArrayList<Tuple>();
    List<String> inputValueNameList;
    List<String> results;
//    List<>
    private OutputCollector collector;
    protected JoinBolt() {
        super();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.collector = collector;
        results = new ArrayList<>();
        Map<String, Map<String, List<String>>> inputFields = context.getThisInputFields();
        Iterator<String> iter = inputFields.keySet().iterator();
        while (iter.hasNext()){
            String key = iter.next();
            Map<String, List<String>> val = inputFields.get(key);
            Iterator<String> iter2 = val.keySet().iterator();
            while (iter2.hasNext()){
                inputValueNameList = val.get(iter2.next());
                for(String item : inputValueNameList){
                    System.out.println(item);
                }
            }
        }
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        List<Tuple> tuplesInWindow = tupleWindow.get();
        List<Tuple> newTuples = tupleWindow.getNew();
        List<Tuple> expiredTuples = tupleWindow.getExpired();
        LOG.debug("Events in current window: " + tuplesInWindow.size());
        if(expiredTuples.size() > 0){
            System.out.println(newTuples.size());
            System.out.println(expiredTuples.size());
        }
        String OriginTabName = JoinCondition.originTabName;
        String JoinTabName = "";
        Iterator<String> iterator = JoinCondition.linkTablemap.keySet().iterator();
        String JoinOP = "";
        String compareCol = "";
        while (iterator.hasNext()){
            String A = iterator.next();
            JoinTabName = A.split("_")[0];
            JoinOP = A.split("_")[1];
            compareCol = JoinCondition.linkTablemap.get(A).getTcItemRight().getColName();
        }
        for(Tuple tuple: newTuples){
            if(tuple.getValue(0) == OriginTabName){
                List_originTab.add(tuple);
            }
            else if(tuple.getValue(0) == JoinTabName){
                List_joinTab.add(tuple);
            }
        }
        boolean flag = false;
        if(JoinOP == "LEFT JOIN"){
            for(int i = 0;i<List_originTab.size();i++){
                for(int j = 0;j<List_joinTab.size();j++){
                    String B = "";
                    if(List_joinTab.get(j).getValueByField(compareCol) == List_originTab.get(i).getValueByField(compareCol)){
                        for(int ii = 0;ii < List_originTab.get(i).size();ii++ ){
                            B += List_originTab.get(i).getString(ii) + ",";
                        }
                        for(int jj = 0;jj < List_joinTab.get(j).size();jj++ ){
                            B += List_joinTab.get(j).getString(jj) + ",";
                        }
                        List_joinTab.remove(j);
                        results.add(B);
                        flag = true;
                        break;
                    }

                }
                if(flag == false){
                    String B = "";
                    for(int ii = 0;ii < List_originTab.get(i).size();ii++ ){
                        B += List_originTab.get(i).getString(ii) + ",";
                    }
                    results.add(B);
                }else {
                    flag = false;
                }
            }
        }

    }

    @Override
    public void cleanup() {
        super.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return super.getComponentConfiguration();
    }
}
