package topology;

import ParseOfSQL.ParsingSQL;
import bolts.JoinBolt;
import bolts.PrinterBolt;
import definetable.Global;
import definetable.MField;
import definetable.MTable;
import domain.BinaryTreeAndOr;
import domain.ProjectConfig;
import operation.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import spouts.JoinSpouttest_1;
import spouts.JoinSpouttest_2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 *
 * Created by yao on 5/16/17.
 */
public class JoinTopology {
    private static HashMap<String, MTable> dataBase;
    private static ParsingSQL parsingSQL;
    private static FileReader fileReader;

    public static void main(String[] args) throws Exception {
        //region   载入表结构
        Global.loadingDataStructure(ProjectConfig.res_createTab_path);
        //测试是否载入成功,并获取表的列名
        dataBase = Global.DataBase;
        //endregion
        fileReader = new FileReader(ProjectConfig.res_inputSQL_path);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String sql = "";
        parsingSQL = new ParsingSQL();
        boolean flag = true;
        while (flag && (sql = bufferedReader.readLine()) != null) {
            // 读取sql ,解析sql
            System.out.println(sql);
            parsingSQL.testparsingTheSQL(sql);

            //region 测试SQL 语义是否解析成功
            System.out.println(Selection.binaryTreeAndOr == null);
            BinaryTreeAndOr binaryTreeAndOr = Selection.binaryTreeAndOr;
            System.out.println(Projection.proList.size());
            List<TCItem> tp = Projection.proList;
            System.out.println(Selection.fromTableList.size());
            List<String> ft = Selection.fromTableList;
            System.out.println(JoinCondition.linkTablemap.size());
            Map<String, JoinTwoTable> tj = JoinCondition.linkTablemap;
            System.out.print(GroupBy.groupList.size());
            List<TCItem> tg = GroupBy.groupList;
            System.out.println(AggregationStream.agreFunList.size());
            List<AgregationFunFactor> ag = AggregationStream.agreFunList;
            System.out.println(AggregationStream.havingList.size());
            List<TCItem> th = AggregationStream.havingList;
            // endregion
            //只读取一条sql语句
            flag = false;
        }
        bufferedReader.close();
        fileReader.close();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("data_reader_1",new JoinSpouttest_1());
        builder.setSpout("data_reader_2",new JoinSpouttest_2());
        JoinBolt bolt = (JoinBolt) new JoinBolt().withTumblingWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS));
        PrinterBolt bolt2 = new PrinterBolt();

        List<String> JoinOutputField = JoinDataStruct();

        bolt.setDescOfOutputFileds(JoinOutputField);

        builder.setBolt("join_bolt",bolt,1).shuffleGrouping("data_reader_1").shuffleGrouping("data_reader_2");
        builder.setBolt("print_bolt",bolt2,1).shuffleGrouping("join_bolt");


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


    private static List<String> JoinDataStruct(){
        List<String> JoinOutputField = new ArrayList<String>();
        JoinOutputField.add("Table");

        String JoinTabName = "";  // 获取连接表表名
        Iterator<String> iterator = JoinCondition.linkTablemap.keySet().iterator();
        String JoinOP = "";  // join的方式，是Left，Right或者Inner
        String compareCol = ""; // 获取连接的条件，如JData_Action_201602.sku_id = JData_Action_201603.sku_id，则该项为sku_id
        while (iterator.hasNext()){
            String A = iterator.next();
            JoinTabName = A.split("\\|")[0];
            JoinOP = A.split("\\|")[1];
            compareCol = JoinCondition.linkTablemap.get(A).getTcItemRight().getColName();
        }
        MTable mTable_1 = dataBase.get(JoinCondition.originTabName);
        MTable mTable_2 = dataBase.get(JoinTabName);

        if(JoinOP.equals("Left") || JoinOP.equals("Inner")){
            for(MField mField:mTable_1.getField()){
                JoinOutputField.add(JoinCondition.originTabName + "." + mField.getName());
            }
            for(MField mField:mTable_2.getField()){
                if(mField.getName().equals(compareCol))
                    continue;
                JoinOutputField.add(JoinTabName + "." + mField.getName());
            }
        }
        else if(JoinOP.equals("Right")){
            for(MField mField:mTable_2.getField()){
                JoinOutputField.add(JoinTabName + "." + mField.getName());
            }
            for(MField mField:mTable_1.getField()){
                if(mField.getName().equals(compareCol))
                    continue;
                JoinOutputField.add(JoinCondition.originTabName + "." + mField.getName());
            }
        }
        return JoinOutputField;
    }

}
