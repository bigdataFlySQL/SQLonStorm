package spouts;

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

import definetable.Global;
import definetable.MField;
import definetable.MTable;
import domain.ProjectConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


import ParseOfSQL.ParsingSQL;
import domain.BinaryTreeAndOr;
import operation.*;

public class StreamDataReaderSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;
    private ParsingSQL parsingSQL;
    private String tableName;
    private List<String> descOfOutputFileds;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getDescOfOutputFileds() {
        return descOfOutputFileds;
    }

    public void setDescOfOutputFileds(List<String> descOfOutputFileds) {
        this.descOfOutputFileds = descOfOutputFileds;
    }

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
        // MySQL的JDBC URL编写方式：jdbc:mysql://主机名称：连接端口/数据库的名称?参数=值
        // 避免中文乱码要指定useUnicode和characterEncoding
        // 执行数据库操作之前要在数据库管理系统上创建一个数据库，名字自己定，
        // 下面语句之前就要先创建javademo数据库
        String url = "jdbc:mysql://localhost:3306/jingdongdata?"
                + "user=" + ProjectConfig.mySQL_user + "&password=" + ProjectConfig.mySQL_passwd + "&useUnicode=true&characterEncoding=UTF8";

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
            // 选择用户购物数据的2月份的数据表
            String sql = "SELECT * FROM "+this.tableName+" LIMIT 0, 10";
            ResultSet rs = statement.executeQuery(sql);
            System.out.println("-----------------");

            int msgid = 1;
            while (rs.next()) {

                Values emitVal = new Values();
                //输出第一项为表名
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
        this.collector = collector;
    }


    /**
     * spout 的 output fields 为 表的列名String集合
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        Fields fields = new Fields(this.descOfOutputFileds);
        declarer.declare(fields);

    }
}
