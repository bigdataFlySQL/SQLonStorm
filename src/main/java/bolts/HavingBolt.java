package bolts;

import operation.AggregationStream;
import operation.TCItem;
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

//        System.out.println(input.size());
        // 获取这次tuple的表名
        String curTableName = input.getStringByField("Table");
        if (!AggregationStream.havingList.isEmpty()){
            // SQL 有having 需求
            boolean isPassing = false;
            for (TCItem tcItem:AggregationStream.havingList){
                if (curTableName.equals(tcItem.getTableName())){
                    // 要比较属性的表名相同
                    // 从当前tuple中获取表达式左边待比较的值
                    Integer compLeftVal = Integer.valueOf((String) input.getValueByField(tcItem.getColName()));
                    Integer compRightVal = tcItem.getComIntVal();
                    // 获取比较符号 >,<,=,!=
                    String compOperator = tcItem.getComparator();
                    if (compOperator.equals(">")) {
                        if (compLeftVal > compRightVal) {
                            isPassing = true;
                        }
                    } else if (compOperator.equals("<")) {
                        if (compLeftVal < compRightVal) {
                            isPassing = true;
                        }

                    } else if (compOperator.equals("=")) {
                        if (compLeftVal.equals(compRightVal) ) {
                            isPassing = true;
                        }
                    } else if (compOperator.equals("!=")) {
                        if (!compLeftVal.equals(compRightVal)) {
                            isPassing = true;
                        }
                    }
                }
            }
            if (isPassing){
                collector.emit(input.getValues());
            }
        }else{
            // SQL 没有having 需求，直接流过
            collector.emit(input.getValues());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields fields = new Fields(this.descOfOutputFileds);
        declarer.declare(fields);
    }
}
