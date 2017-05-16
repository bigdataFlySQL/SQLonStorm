package bolts;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SelectBolt extends BaseBasicBolt {

    private List<String> results;
    private FileReader fileReader;

    public void prepare(Map stormConf, TopologyContext context) {

        results = new ArrayList<>();

        // 获取StreamSataReaderSpout 传过来的每一行数据的属性信息 如 user_id,sku_id,cate
        Map<String, Map<String, List<String>>> inputFields = context.getThisInputFields();
        Iterator<String> iter = inputFields.keySet().iterator();
        while (iter.hasNext()) {
            String key = iter.next();
            Map<String, List<String>> val = inputFields.get(key);
            Iterator<String> iter2 = val.keySet().iterator();
            while (iter2.hasNext()) {
                List<String> valList = val.get(iter2.next());
                for (String item : valList) {
                    System.out.println(item);
                }
            }

        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return super.getComponentConfiguration();
    }

    public void cleanup() {
        System.out.println("打印结果");
        for (String item : results) {
            System.out.println(item);
        }
    }

    /**
     * 执行SQL 选择操作
     */
    public void execute(Tuple input, BasicOutputCollector collector) {
        //选择商品类型为8的数据
        if (Integer.valueOf(input.getString(5)) == 8) {
            String ansStr = "";
            for (int i = 0; i < input.size(); i++) {
                ansStr += input.getString(i) + ",";
            }

            results.add(ansStr);
        }

    }


    /**
     * 输出元组数据到下一个bolt
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
