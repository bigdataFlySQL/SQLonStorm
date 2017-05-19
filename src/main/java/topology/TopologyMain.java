package topology;

import ParseOfSQL.ParsingSQL;
import bolts.GroupByBolt;
import bolts.HavingBolt;
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
import java.util.*;


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

            // 获取每个spout 输出tuple 的属性名
            //配置第一个spout 的数据源
            String firstTabName = Selection.fromTableList.get(0);
            List<String> jD_02outFiledNameList = loadDataStruct(firstTabName);

            StreamDataReaderSpout sDataSpout = new StreamDataReaderSpout();
            sDataSpout.setTableName(firstTabName);
            //设置该spout的tuple输出属性名
            sDataSpout.setDescOfOutputFileds(jD_02outFiledNameList);
            // 有join需求
            StreamDataReaderSpout sjoinTableDataSpout = null;
            List<String> secSpoutOutFiledList = null;
            HashMap<String,JoinTwoTable> joinTwoTabMap = JoinCondition.linkTablemap;
            if (!joinTwoTabMap.isEmpty()){
                sjoinTableDataSpout = new StreamDataReaderSpout();
                String joinTableName = null; // 所连接的表，目前仅支持连一个表
                Iterator<String> keyIter = joinTwoTabMap.keySet().iterator();
                while (keyIter.hasNext()){
                    joinTableName = keyIter.next();
                }
                secSpoutOutFiledList = loadDataStruct(joinTableName);
                sjoinTableDataSpout = new StreamDataReaderSpout(); // 初始化join表的数据源
                sjoinTableDataSpout.setTableName(joinTableName); // 指定数据源来自的表名
                sjoinTableDataSpout.setDescOfOutputFileds(secSpoutOutFiledList);

            }

            //选择
            SelectBolt selectBolt = new SelectBolt();
            selectBolt.setDescOfOutputFileds(jD_02outFiledNameList);

            // 分组 group by
            GroupByBolt groupByBolt = new GroupByBolt();
            List<String> groupByOutputFields = new ArrayList<>(jD_02outFiledNameList);
            if (!AggregationStream.agreFunList.isEmpty()){
                // SQL 有分组的需求
                for (AgregationFunFactor funFactor: AggregationStream.agreFunList){
                    groupByOutputFields.add(funFactor.getFunFullName());
                }
            }
            groupByBolt.setDescOfOutputFileds(groupByOutputFields);

            // Having
            HavingBolt havingBolt = new HavingBolt();
            // HavingBolt 的输出属性和GroupByBolt 一样
            havingBolt.setDescOfOutputFileds(groupByOutputFields);

            //映射
            ProjectionBolt projectionBolt = new ProjectionBolt(ProjectConfig.projection_result_file_path);


//		builder.setBolt("word-counter", new WordCounter(),2)
//				.shuffleGrouping("word-normalizer");
//		builder.setBolt("word-counter", new WordCounter(),2)
//			.fieldsGrouping("word-normalizer", new Fields("word"));

            //Topology definition
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("data-reader", sDataSpout);
            builder.setBolt("select", selectBolt).shuffleGrouping("data-reader");
            builder.setBolt("group-by",groupByBolt).shuffleGrouping("select");
            builder.setBolt("having", havingBolt).shuffleGrouping("group-by");
            builder.setBolt("projection", projectionBolt).shuffleGrouping("having");

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

    /**
     *  获取 joinBolt 输出的outFields
     * @return joinBolt 输出的outField
     */
    private static List<String> joinDataStruct(){
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
