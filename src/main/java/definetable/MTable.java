package definetable;

import java.util.ArrayList;
//import java.util.List;

/**
 * Created by yao on 5/10/17.
 */
public class MTable {
    private String tablename;
    private String database_name;
    private ArrayList<MField> fields;
    private int size;

    /**
     * set_mTable函数，设置函数
     * @param tablename 表名
     * @param database_name 数据库名
     * @param size 表的大小（列数）
     * @param fields 每一列的信息，包括列名，列中数据类型，默认值
     */
    public void set_mTable(String tablename,String database_name,int size, ArrayList<MField> fields){
        this.tablename = tablename;
        this.database_name = database_name;
        this.size = size;
        this.fields = fields;
    }

    /**
     * table_attr_Size函数，返回该表中的列的个数
     * @return
     */
    public int table_attr_Size(){
        return this.size;
    }

    /**
     * getName函数，获取数据库和表名
     * @return 返回值为字符串数组
     */
    public String[] getName(){
        String []Database_Table_name = new String[2];
        Database_Table_name[0] = this.database_name;
        Database_Table_name[1] = this.tablename;
        return Database_Table_name;
    }

    /**
     * getField函数，获取每一个表格的域信息
     * @return 返回值是存放各个域的ArrayList
     */
    public ArrayList<MField> getField(){
        return this.fields;
    }


}
