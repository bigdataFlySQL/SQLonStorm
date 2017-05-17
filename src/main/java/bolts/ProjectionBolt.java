package bolts;

import operation.Projection;
import operation.TCItem;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by yuxiao on 5/16/17.
 * 处理映射操作的Bolt
 */
public class ProjectionBolt extends BaseBasicBolt {
    // 记录最后处理结果
    private List<String> results;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        results = new ArrayList<>();
    }

    @Override
    public void cleanup() {
        super.cleanup();
        // 最后的映射结果
        System.out.println("映射的结果");
        for (String item:results){
                System.out.println(item);
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // 获取这次tuple的表名
        String curTableName = tuple.getStringByField("Table");
        List<TCItem> proList = Projection.proList;
        if (!proList.isEmpty()){
            StringBuffer sb = new StringBuffer();
            // SQL 有映射语义
            for (TCItem tcItem: proList){
                //先处理选择全部的特殊语义
                if (tcItem.getColName().equals("*")){
                    for (Object object : tuple.getValues()){
                        sb.append((String)object+",");
                    }
                    this.results.add(sb.toString());
                    break;// 目前仅支持 select * 后面没有其他映射条件
                }

                if (curTableName.equals(tcItem.getTableName())){
                    //表名相同,获取相对应的值
                    String getTargetVal = tuple.getStringByField(tcItem.getColName())+",";
                    sb.append(getTargetVal);
                }
            }
            if (!sb.toString().isEmpty()){
                this.results.add(sb.toString());
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
