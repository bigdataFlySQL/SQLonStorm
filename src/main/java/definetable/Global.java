package definetable;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;


/**
 * Created by yao on 5/10/17.
 */
public class Global {
    private CCJSqlParserManager parserManager = new CCJSqlParserManager();
    public static HashMap<String, MTable> DataBase = new HashMap<String, MTable>();

    /**
     * configurationData函数，辅助填充Database的hashmap函数
     * @param statement 对应的文件中的一条创建数据库请求
     * @throws JSQLParserException
     */
    public void configurationData(String statement) throws JSQLParserException {
        CreateTable createTable = (CreateTable) parserManager.parse(new StringReader(statement));
        MTable mtable = new MTable();
        ArrayList<MField> mFields = new ArrayList<MField>(); // field信息用ArrayList进行构造

        for(int i = 0;i < createTable.getColumnDefinitions().size();i++){ // 填充该mtable的field信息
            MField mField = new MField();
            mField.setField(((ColumnDefinition) createTable.getColumnDefinitions().get(i)).getColumnName().split("`")[1], ((ColumnDefinition) createTable.getColumnDefinitions().get(i)).getColumnSpecStrings().get(1),((ColumnDefinition)createTable.getColumnDefinitions().get(i)).getColDataType().toString().split(" ")[0]);
            mFields.add(mField);
        }
        // 填充mtable
        mtable.set_mTable(createTable.getTable().getName().split("`")[1],"JData", createTable.getColumnDefinitions().size(),mFields);
        // 填充hashmap
        this.DataBase.put(createTable.getTable().getName().split("`")[1], mtable);

    }

    /**
     * setConfigurationdata函数，填充数据库表信息的函数
     * @param file_path 创建数据库信息的文件的路径
     * @throws IOException
     * @throws JSQLParserException
     */
    public void setConfigurationdata(String file_path) throws IOException, JSQLParserException{
        File input_file = new File(file_path);
        String CreateTableQuery = ""; // 截取文件中的create table的query
        InputStreamReader read = new InputStreamReader(new FileInputStream(input_file));
        BufferedReader bufferedReader = new BufferedReader(read);

        String line = null;
        while((line = bufferedReader.readLine())!=null) { // 每次读取一行文本知道文件被读完
            CreateTableQuery = CreateTableQuery + line;
            if (line.endsWith(";")) { // 遇到分号表示获取了文件中的一个完整创建数据库请求
                configurationData(CreateTableQuery); // 针对这个请求填充hashmap
                CreateTableQuery = ""; // 清空请求，准备重新构造请求
            }
        }

    }

    public static void main(String[] args) throws IOException,JSQLParserException{
        Global configurationFileWrite = new Global();
        configurationFileWrite.setConfigurationdata(args[0]);

        try {

            Iterator it = configurationFileWrite.DataBase.keySet().iterator();
            while (it.hasNext()){
                String key = (String)it.next();
                System.out.println(key);
                MTable m = configurationFileWrite.DataBase.get(key);
                System.out.println(m.getName()[0] + " " + m.getName()[1]);
                System.out.println(m.table_attr_Size());
                ArrayList<MField> mm = m.getField();
                for(int i = 0;i < mm.size(); i++){
                    System.out.println(mm.get(i).getField()[0] + " " +  mm.get(i).getField()[1] + " " + mm.get(i).getField()[2]);
                }

            }
        }catch (Exception e){
            System.out.println("读取文件出错");
        }

    }

}
