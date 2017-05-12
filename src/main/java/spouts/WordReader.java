package spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import ParseOfSQL.ParsingSQL;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import domain.BinaryTreeAndOr;
import domain.BinaryTreeAnrOrNode;
import operation.*;
import test.SelectTest;

public class WordReader extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;
    private ParsingSQL parsingSQL;

    public void ack(Object msgId) {
        System.out.println("OK:" + msgId);
    }

    public void close() {
    }

    public void fail(Object msgId) {
        System.out.println("FAIL:" + msgId);
    }

    /**
     * The only thing that the methods will do It is emit each
     * file line
     */
    public void nextTuple() {
        /**
         * The nextuple it is called forever, so if we have been readed the file
         * we will wait and then return
         */
        if (completed) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                //Do nothing
            }
            return;
        }
//		String str;
//		//Open the reader
//		BufferedReader reader = new BufferedReader(fileReader);
//		try{
//			//Read all lines
//			while((str = reader.readLine()) != null){
//				/**
//				 * By each line emmit a new value with the line as a their
//				 */
//				this.collector.emit(new Values(str),str);
//			}


        // MySQL的JDBC URL编写方式：jdbc:mysql://主机名称：连接端口/数据库的名称?参数=值
        // 避免中文乱码要指定useUnicode和characterEncoding
        // 执行数据库操作之前要在数据库管理系统上创建一个数据库，名字自己定，
        // 下面语句之前就要先创建javademo数据库
        String url = "jdbc:mysql://localhost:3306/jingdongdata?"
                + "user=root&password=12211104&useUnicode=true&characterEncoding=UTF8";

        try {
            Connection conn = null;
            // 之所以要使用下面这条语句，是因为要使用MySQL的驱动，所以我们要把它驱动起来，
            // 可以通过Class.forName把它加载进去，也可以通过初始化来驱动起来，下面三种形式都可以
            Class.forName("com.mysql.cj.jdbc.Driver");// 动态加载mysql驱动
            // or:
            // com.mysql.jdbc.Driver driver = new com.mysql.jdbc.Driver();
            // or：
            // new com.mysql.jdbc.Driver();

            System.out.println("成功加载MySQL驱动程序");
            // 一个Connection代表一个数据库连接
            conn = DriverManager.getConnection(url);


            Statement statement = conn.createStatement();
            String sql = "SELECT * FROM JData_Product LIMIT 0, 10";
            ResultSet rs = statement.executeQuery(sql);
            System.out.println("-----------------");

            String name = null;
            while (rs.next()) {
                String tempStr = "";
                tempStr += rs.getInt("sku_id") + "|";

//                System.out.println(rs.getInt("sku_id"));
                tempStr += rs.getInt("attr1") + "|";
                tempStr += rs.getInt("attr2");
                System.out.println(tempStr);
                this.collector.emit(new Values(tempStr), tempStr);
            }

            conn.close();
        } catch (Exception e) {
            throw new RuntimeException("Error reading tuple", e);
        } finally {
            completed = true;

        }
    }

    /**
     * We will create the file and get the collector object
     */
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        try {
            this.fileReader = new FileReader(conf.get("InputSQL").toString());
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String sql = "";
            parsingSQL = new ParsingSQL();
            while ((sql = bufferedReader.readLine()) != null) {
                // 读取sql ,解析sql
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
                Map<String,JoinTwoTable> tj = JoinCondition.linkTablemap;
                System.out.print(GroupBy.groupList.size());
                List<TCItem> tg = GroupBy.groupList;
                System.out.println(AggregationStream.agreFunList.size());
                List<AgregationFunFactor> ag = AggregationStream.agreFunList;
                // endregion
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file [" + conf.get("wordFile") + "]");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.collector = collector;
    }


    /**
     * Declare the output field "word"
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }
}
