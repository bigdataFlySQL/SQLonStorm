package topology;

import ParseOfSQL.ParsingSQL;
import bolts.ProjectionBolt;
import definetable.Global;
import definetable.MField;
import definetable.MTable;
import domain.BinaryTreeAndOr;
import domain.ProjectConfig;
import operation.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import spouts.StreamDataReaderSpout;


import bolts.SelectBolt;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TopologyMain {
    private static ParsingSQL parsingSQL;
    private static FileReader fileReader;
    private static HashMap<String, MTable> dataBase;


    public static void main(String[] args) throws InterruptedException {

        try {

            //region   载入表结构
            Global.loadingDataStructure(ProjectConfig.res_createTab_path);
            //测试是否载入成功,并获取表的列名
            dataBase = Global.DataBase;
            //endregion

            // 获取每个spout 输出tuple 的属性名
            List<String> jD_02outFiledNameList = loadDataStruct("JData_Action_201602");

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


            //Topology definition
            TopologyBuilder builder = new TopologyBuilder();
            StreamDataReaderSpout sDataSpout = new StreamDataReaderSpout();
            //设置该spout的tuple输出属性名
            sDataSpout.setDescOfOutputFileds(jD_02outFiledNameList);
            builder.setSpout("data-reader", sDataSpout);
            //选择
            SelectBolt selectBolt = new SelectBolt();
            selectBolt.setDescOfOutputFileds(jD_02outFiledNameList);
            builder.setBolt("select", selectBolt)
                    .shuffleGrouping("data-reader");

            //映射
            ProjectionBolt projectionBolt = new ProjectionBolt();
            builder.setBolt("projection", projectionBolt).shuffleGrouping("select");
//		builder.setBolt("word-counter", new WordCounter(),2)
//				.shuffleGrouping("word-normalizer");
//		builder.setBolt("word-counter", new WordCounter(),2)
//			.fieldsGrouping("word-normalizer", new Fields("word"));

            //Configuration
            Config conf = new Config();
            conf.setDebug(false);
            //Topology run
            conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
            Thread.sleep(8000);
            cluster.shutdown();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file ");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }



    }

    /**
     *
     * @param tableName
     * @return List<String> descOfOutputFileds
     */
    private static List<String> loadDataStruct(String tableName) {
        List<String> descOfOutputFileds = new ArrayList<String>();
        try {

            MTable jData_Action_201602 = dataBase.get(tableName);
            //输出第0项为表名
            descOfOutputFileds.add("Table");
            for (MField mField : jData_Action_201602.getField()) {
                //获取表的列名，作为spout 的 output fields
                descOfOutputFileds.add(mField.getName());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return descOfOutputFileds;

    }
}
