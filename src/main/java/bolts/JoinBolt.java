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
    public JoinBolt() {
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

        String OriginTabName = JoinCondition.originTabName;  // 获取被连接表表名
        String JoinTabName = "";  // 获取连接表表名
        Iterator<String> iterator = JoinCondition.linkTablemap.keySet().iterator();
        String JoinOP = "";  // join的方式，是Left，Right或者Inner
        String compareCol = ""; // 获取连接的条件，如JData_Action_201602.sku_id = JData_Action_201603.sku_id，则该项为sku_id
        while (iterator.hasNext()){
            String A = iterator.next();
            JoinTabName = A.split("\\|")[0];
            JoinOP = A.split("\\|")[1];
            compareCol = JoinCondition.linkTablemap.get(A).getTcItemRight().getColName();
//            results.add("hello world1");
            System.out.println(A + "  " +OriginTabName+ "  " + JoinTabName + "  "+ JoinOP+ "  " + compareCol);
        }
        for(Tuple tuple: newTuples){
            System.out.println(tuple.getValue(0));
            if(tuple.getValue(0).toString().equals(OriginTabName)){
                List_originTab.add(tuple);

            }
            else if(tuple.getValue(0).toString().equals(JoinTabName)){
                List_joinTab.add(tuple);

            }
        }
        boolean flag = false;
        if(JoinOP.equals("Left")) {
            Left_Join(compareCol);
        }
        else if(JoinOP.equals("Right")){
            Right_Join(compareCol);
        }
        else if(JoinOP.equals("Inner")){
            Inner_Join(compareCol);
        }
    }

    public void Left_Join(String compareCol){
        boolean flag = false;
        for(int i = 0;i<List_originTab.size();i++){
            for(int j = 0;j<List_joinTab.size();j++){
                String B = "";
                if(List_joinTab.get(j).getValueByField(compareCol).equals(List_originTab.get(i).getValueByField(compareCol))){
                    for(int ii = 0;ii < List_originTab.get(i).size();ii++ ){
                        if(ii == 0)
                            B += List_originTab.get(i).getString(ii) + ",";
                        else
                            B += List_originTab.get(i).getString(0) + "." + List_originTab.get(i).getString(ii) + ",";
                    }
                    for(int jj = 1;jj < List_joinTab.get(j).size();jj++ ){
                        if(List_joinTab.get(j).getFields().get(jj).equals(compareCol))
                            continue;
                        if(jj == List_joinTab.get(j).size() - 1) {
                            B += List_joinTab.get(j).getString(0) + "." + List_joinTab.get(j).getString(jj);
                            continue;
                        }
                        B += List_joinTab.get(j).getString(0) + "." + List_joinTab.get(j).getString(jj) + ",";
                    }
                    results.add(B);
                    flag = true;
                }

            }
            if(flag == false){
                String B = "";
                for(int ii = 0;ii < List_originTab.get(i).size();ii++ ){
                    if(ii == 0)
                        B += List_originTab.get(i).getString(ii) + ",";
                    else
                        B += List_originTab.get(i).getString(0) + "." + List_originTab.get(i).getString(ii) + ",";
                }
                for(int jj = 0;jj < List_joinTab.get(0).size() - 2;jj++){
                    if(jj == List_joinTab.get(0).size() - 3) {
                        B += List_joinTab.get(0).getString(0) + ".";
                        continue;
                    }
                    B += List_joinTab.get(0).getString(0) + ".,";
                }
                results.add(B);
            }else {
                flag = false;
            }
        }
    }

    public void Right_Join(String compareCol){
        boolean flag = false;
        for(int i = 0;i<List_joinTab.size();i++){
            for(int j = 0;j<List_originTab.size();j++){
                String B = "";
                if(List_originTab.get(j).getValueByField(compareCol).equals(List_joinTab.get(i).getValueByField(compareCol))){
                    for(int ii = 0;ii < List_joinTab.get(i).size();ii++ ){
                        if(ii == 0)
                            B += List_originTab.get(j).getString(0) + ",";
                        else
                            B += List_joinTab.get(i).getString(0) + "." + List_joinTab.get(j).getString(ii) + ",";
                    }
                    for(int jj = 1;jj < List_originTab.get(j).size();jj++ ){
                        B += List_originTab.get(j).getString(0) + "." + List_originTab.get(j).getString(jj) + ",";
                    }
                    results.add(B);
                    flag = true;
                }

            }
            if(flag == false){
                String B = "";
                for(int ii = 0;ii < List_joinTab.get(i).size();ii++ ){
                    if(ii==0){
                        B += List_originTab.get(0).getString(0);
                    }

                    else {
                        B += List_joinTab.get(i).getString(ii) + "." + List_joinTab.get(i).getString(ii) + ",";
                    }
                }
                results.add(B);
            }else {
                flag = false;
            }
        }
    }

    public void Inner_Join(String compareCol){
        for(int i = 0;i<List_originTab.size();i++){
            for(int j = 0;j<List_joinTab.size();j++){
                String B = "";
                if(List_joinTab.get(j).getValueByField(compareCol).equals(List_originTab.get(i).getValueByField(compareCol))){
                    for(int ii = 0;ii < List_originTab.get(i).size();ii++ ){
                        if(ii == 0)
                            B += List_originTab.get(i).getString(ii) + ",";
                        else
                            B += List_originTab.get(i).getString(0) + "." + List_originTab.get(i).getString(ii) + ",";
                    }
                    for(int jj = 1;jj < List_joinTab.get(j).size();jj++ ){
                        if(List_joinTab.get(j).getFields().get(jj).equals(compareCol))
                            continue;
                        if(jj == List_joinTab.get(j).size() - 1) {
                            B += List_joinTab.get(j).getString(0) + "." + List_joinTab.get(j).getString(jj);
                            continue;
                        }
                        B += List_joinTab.get(j).getString(0) + "." + List_joinTab.get(j).getString(jj) + ",";
                    }
                    results.add(B);
                }
            }
        }
    }


    @Override
    public void cleanup() {

        System.out.println("打印结果");
        for(String item : results){
            System.out.println(item);
            System.out.println("---------------------------------------------");
        }
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
