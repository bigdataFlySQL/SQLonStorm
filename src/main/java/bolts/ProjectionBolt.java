package bolts;

import operation.AggregationStream;
import operation.AgregationFunFactor;
import operation.Projection;
import operation.TCItem;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by yuxiao on 5/16/17.
 * 处理映射操作的Bolt
 */
public class ProjectionBolt extends BaseBasicBolt {
    // 记录最后处理结果
    private List<String> results;
    private List<String> inputvalueNameList;

    private List<String> descOfOutputFileds;
    private String filePath;
    private boolean flag = true;


    public ProjectionBolt(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        results = new ArrayList<>();
        descOfOutputFileds = new ArrayList<>();
        inputvalueNameList = new ArrayList<>();

        // 获取HavingBolt 传过来的每一行数据的属性信息 如 user_id,sku_id,cate
        Map<String, Map<String, List<String>>> inputFields = context.getThisInputFields();
        Iterator<String> iter = inputFields.keySet().iterator();
        while (iter.hasNext()) {
            String key = iter.next();
            Map<String, List<String>> val = inputFields.get(key);
            Iterator<String> iter2 = val.keySet().iterator();
            while (iter2.hasNext()) {
                inputvalueNameList = val.get(iter2.next());
                for (String item : inputvalueNameList) {
                    System.out.println(item);
                }
            }

        }
    }

    @Override
    public void cleanup() {
        super.cleanup();

        try {
            // 最后的映射结果
            FileWriter fileWriter = new FileWriter(this.filePath);
            BufferedWriter bw = new BufferedWriter(fileWriter);
            bw.write("映射的结果\n");
            for (String item : this.descOfOutputFileds) {
                bw.write(item + "| ");
            }
            bw.write('\n');
            for (String item : results) {
                bw.write(item);
                bw.write('\n');
            }
            bw.close();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // 获取这次tuple的表名
        String curTableName = tuple.getStringByField("Table");
        List<TCItem> proList = Projection.proList;
        StringBuffer sb = new StringBuffer();
        if (!proList.isEmpty()) {
            // SQL 有映射语义
            for (TCItem tcItem : proList) {
                //先处理选择全部的特殊语义
                if (tcItem.getColName().equals("*")) {
                    for (Object object : tuple.getValues()) {
                        sb.append((String) object + ",");
                    }
                    this.results.add(sb.toString());
                    break;// 目前仅支持 select * 后面没有其他映射条件
                }
                if (curTableName.contains("|")) {
                    // 处理join
                    String tempKeyFieldName = tcItem.getTableName() + "." + tcItem.getColName();
                    if (flag) {
                        this.descOfOutputFileds.add(tempKeyFieldName);
                    }
                    String tVal =  tVal = tuple.getStringByField(tempKeyFieldName);
                    String getTargetVal = tVal + ",";
                    sb.append(getTargetVal);
                }else{
                    if (curTableName.equals(tcItem.getTableName())) {
                    //表名相同,获取相对应的值
                    if (flag){
                        this.descOfOutputFileds.add(curTableName+"."+tcItem.getColName());
                    }
                    String getTargetVal = tuple.getStringByField(tcItem.getColName()) + ",";
                    sb.append(getTargetVal);
                    }
                }
            }

        }

        // 处理聚合映射
        for (AgregationFunFactor funFactor : AggregationStream.agreFunList) {
            if (flag) {
                this.descOfOutputFileds.add(funFactor.getFunFullName());
            }
            sb.append(tuple.getStringByField(funFactor.getFunFullName()));
        }

        flag = false;// 保证 descOfOutputFileds 只获取一次输出属性名
        if (!sb.toString().isEmpty()) {
            this.results.add(sb.toString());
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
