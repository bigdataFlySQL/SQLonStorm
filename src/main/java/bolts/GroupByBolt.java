package bolts;

import operation.AggregationStream;
import operation.AgregationFunFactor;
import operation.GroupBy;
import operation.TCItem;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.*;

/**
 * Created by yuxiao on 5/17/17.
 * GroupBy 语义过滤Bolt
 */
public class GroupByBolt extends BaseWindowedBolt {

    private int count = 0;

    // 记录要分组后的数据 key:分组的条件值，value: 某一组的tuple list
    private Map<String, List<Tuple>> groupByMap;

    private List<String> descOfOutputFileds;
    private OutputCollector collector;

    public List<String> getDescOfOutputFileds() {
        return descOfOutputFileds;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.collector = collector;
        this.groupByMap = new HashMap<>();
    }


    @Override
    public void execute(TupleWindow tupleWindow) {
        boolean isJoin = false;
        List<Tuple> newTuples = tupleWindow.getNew();
        if (!GroupBy.groupList.isEmpty()) {
            this.groupByMap.clear();// 清空上一次的记录
            // SQL 有group by 需求
            //region 接收 tuple
            for (Tuple input : newTuples) {
                String curTableName = input.getStringByField("Table");
                StringBuffer sb = new StringBuffer();
                if (curTableName.contains("|")) {
                    isJoin = true;
                    // 处理 join
                    for (TCItem tcItem : GroupBy.groupList) {
                        // 表名.属性名
                        String keyStr = tcItem.getTableName() + "." + tcItem.getColName();
                        sb.append(input.getStringByField(keyStr));
                    }
                } else {
                    for (TCItem tcItem : GroupBy.groupList) {
                        if (curTableName.equals(tcItem.getTableName())) {
                            //表名相同
                            sb.append(input.getStringByField(tcItem.getColName()));
                        }
                    }
                }

                String tempKey = sb.toString();
                if (this.groupByMap.containsKey(tempKey)) {
                    this.groupByMap.get(tempKey).add(input);
                } else {
                    List<Tuple> newTupleList = new ArrayList<>();
                    newTupleList.add(input);
                    this.groupByMap.put(tempKey, newTupleList);
                }

            }
            //endregion

            //region 发送tuple
            Iterator<String> iterKey = this.groupByMap.keySet().iterator();
            while (iterKey.hasNext()) {
                List<Tuple> valTupleList = this.groupByMap.get(iterKey.next());
                // 将要emit 给下一个bolt 的tuple
                List<Object> emitVal = valTupleList.get(0).getValues();
                // 进行group by 需要的聚合计算
                if (!AggregationStream.agreFunList.isEmpty()) {
                    for (AgregationFunFactor funFactor : AggregationStream.agreFunList) {
                        if (funFactor.getFunStr().equals("count")) {
                            int resCount = getResultOfCount(isJoin,valTupleList, funFactor.getParameterList());
                            // 把 count 聚合的结果加入到要发送的tuple 中
                            emitVal.add(String.valueOf(resCount));
                        } else if (funFactor.getFunStr().equals("max")) {
                            int resMax = getResultofMax(isJoin,valTupleList, funFactor.getParameterList().get(0));
                            // // 把 max 聚合的结果加入到要发送的tuple 中
                            emitVal.add(String.valueOf(resMax));
                        }
                    }
                }
                // 把一个组的聚合结果发送给HavingBolt
                collector.emit(emitVal);
            }
            //endregion

        } else {
            for (Tuple tuple : newTuples) {
                this.collector.emit(tuple.getValues());
            }
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }

    public void setDescOfOutputFileds(List<String> descOfOutputFileds) {
        this.descOfOutputFileds = descOfOutputFileds;
    }


//    public void execute(Tuple input, BasicOutputCollector collector) {
//        count++;
//        if (!GroupBy.groupList.isEmpty()) {
//            // SQL 有group by 需求
//            String curTableName = input.getStringByField("Table");
//            StringBuffer sb = new StringBuffer();
//            for (TCItem tcItem : GroupBy.groupList) {
//                if (curTableName.equals(tcItem.getTableName())) {
//                    //表名相同
//                    sb.append(input.getStringByField(tcItem.getColName()));
//                }
//            }
//
//
//            if (count >= 10) {
//                // 简单版本，10条tuple 做一次group
//                Iterator<String> iterKey = this.groupByMap.keySet().iterator();
//                while (iterKey.hasNext()) {
//                    List<Tuple> valTupleList = this.groupByMap.get(iterKey.next());
//                    // 将要emit 给下一个bolt 的tuple
//                    List<Object> emitVal= valTupleList.get(0).getValues();
//                    // 进行group by 需要的聚合计算
//                    if (!AggregationStream.agreFunList.isEmpty()) {
//                        for (AgregationFunFactor funFactor : AggregationStream.agreFunList) {
//                            if (funFactor.getFunStr().equals("count")) {
//                                 int resCount = getResultOfCount(valTupleList,funFactor.getParameterList());
//                                 // 把 count 聚合的结果加入到要发送的tuple 中
//                                emitVal.add(String.valueOf(resCount));
//                            } else if (funFactor.getFunStr().equals("max")) {
//                                int resMax = getResultofMax(valTupleList,funFactor.getParameterList().get(0));
//                                // // 把 max 聚合的结果加入到要发送的tuple 中
//                                emitVal.add(String.valueOf(resMax));
//                            }
//                        }
//                    }
//                    // 把一个组的聚合结果发送给HavingBolt
//                    collector.emit(emitVal);
//                }
//            }
//        } else {
//            // SQL 没有group by 需求，input 原样流过
//            collector.emit(input.getValues());
//        }
//    }

    /**
     * 聚合函数 count 计算结果
     *
     * @param isJoin        含有join
     * @param tupleList     属于某一个组的tuple list
     * @param parameterList count(tab1.A,tab1.B) 参数值,可支持多个参数。 若tuple 对应某个参数属性的值为空，则该条tuple 不参与count的计算
     * @return 该组tuple count 函数的执行结果
     */
    private int getResultOfCount(boolean isJoin, List<Tuple> tupleList, List<TCItem> parameterList) {
        int ans = 0;
        for (Tuple tuple : tupleList) {
            boolean flag = true;
            for (TCItem tcItem : parameterList) {
                String tempKey = tcItem.getTableName() + "." + tcItem.getColName();
                if (!isJoin) {
                    tempKey = tcItem.getColName();
                }
                if (tuple.getStringByField(tempKey).isEmpty()) {
                    flag = false;
                    break;
                }
            }
            if (flag) {
                ans++;
            }
        }
        return ans;
    }

    /**
     * 聚合函数 max 计算结果
     *@param isJoin 是否含有join
     * @param tupleList 属于某一个组的tuple list
     * @param parameter max(tab1.A) 参数值，目前仅支持一个参数
     * @return 该组tuple max 函数的执行结果
     */
    private int getResultofMax(boolean isJoin, List<Tuple> tupleList, TCItem parameter) {
        int maxRes = Integer.MIN_VALUE;
        for (Tuple tuple : tupleList) {
            String tempKey = parameter.getTableName() + "." + parameter.getColName();
            if (!isJoin) {
                tempKey = parameter.getColName();
            }
            String tValStr = tuple.getStringByField(tempKey);
            if (!tValStr.isEmpty()) {
                int tVal = Integer.valueOf(tValStr);
                maxRes = Math.max(maxRes, tVal);
            }
        }
        return maxRes;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields fields = new Fields(this.descOfOutputFileds);
        declarer.declare(fields);
    }
}
