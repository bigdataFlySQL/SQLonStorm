package spouts;

import ParseOfSQL.ParsingSQL;
import definetable.Global;
import definetable.MField;
import definetable.MTable;
import domain.BinaryTreeAndOr;
import operation.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * Created by yao on 5/16/17.
 */
public class JoinSpouttest_1 extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;
    private ParsingSQL parsingSQL;
    private List<String> descOfOutputFields;
    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector collector) {
        try {
            this.fileReader = new FileReader(conf.get("InputSQL").toString());
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            parsingSQL = new ParsingSQL();
            String sql = "";
            boolean flag = true;

            while(flag && (sql = bufferedReader.readLine()) != null){
                System.out.println(sql);
                parsingSQL.testparsingTheSQL(sql);
                //region 测试SQL 语义是否解析成功
                System.out.println(Selection.binaryTreeAndOr == null);
                BinaryTreeAndOr binaryTreeAndOr = Selection.binaryTreeAndOr;
                System.out.println(Projection.proList.size());
                List<TCItem> tp = Projection.proList;
                System.out.println(Selection.fromTableList.size());
                List<String> ft= Selection.fromTableList;
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
                flag=false;
            }
            bufferedReader.close();
            this.fileReader.close();
        }catch (FileNotFoundException e){
            throw new RuntimeException("Error reading file");
        }catch (IOException e){
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }
        this.collector = collector;
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public void nextTuple() {
        if(completed){
            try{
                Thread.sleep(1000);
            }catch (InterruptedException e){

            }
        }

        String url = "jdbc:mysql://localhost:3306/jingdongdata?"
                + "user=root&password=yao2376098&useUnicode=true&characterEncoding=UTF8";
        try{
            Connection conn = null;
            Class.forName("com.mysql.cj.jdbc.Driver");
            System.out.println("成功加载MySQL驱动程序");
            conn = DriverManager.getConnection(url);
            Statement statement = conn.createStatement();
            String sql = "SELECT * FROM JData_Action_201602 LIMIT 0, 10";
            ResultSet rs = statement.executeQuery(sql);


            int msgid = 1;
            while (rs.next()){
                Values emitVal = new Values();

                emitVal.add("JData_Action_201602");
                emitVal.add(rs.getString(1));
                emitVal.add(rs.getString(2));
                emitVal.add(rs.getString(3));
                emitVal.add(rs.getString(4));
                emitVal.add(rs.getString(5));
                emitVal.add(rs.getString(6));
                emitVal.add(rs.getString(7));

                this.collector.emit(emitVal, msgid++);

            }

            conn.close();
        }catch (Exception e){
            throw new RuntimeException("Error reading tuple", e);
        }finally {
            completed = true;
        }


    }

    @Override
    public void ack(Object msgId) {
        System.out.println("OK "+ msgId);
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("Fail "+ msgId);
        super.fail(msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        try{
            Global.loadingDataStructure("/home/yao/intellij_IDE/work_space/SQLonStorm/src/main/resources/createtabledata.txt");
            HashMap<String, MTable> dataBase = Global.DataBase;
            MTable jData_Action_201602 = dataBase.get("JData_Action_201602");
            this.descOfOutputFields = new ArrayList<String>();
            this.descOfOutputFields.add("Table");
            for(MField mField: jData_Action_201602.getField()){
                this.descOfOutputFields.add(mField.getName());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        Fields fields = new Fields(this.descOfOutputFields);
        outputFieldsDeclarer.declare(fields);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return super.getComponentConfiguration();
    }
}
