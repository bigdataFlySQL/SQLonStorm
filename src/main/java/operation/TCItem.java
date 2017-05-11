package operation;

/**
 * Created by yuxiao on 5/11/17.
 * 封装操作基本元素（tab1.a）的类
 */
public class TCItem {

    private String tableName;//表名
    private String colName;//属性名
    private boolean isCompare; //是否参与比较
    private String comparator;// 比较符号 >,<,=,!=
    private int comIntVal; // 所比较的数值

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public boolean isCompare() {
        return isCompare;
    }

    public void setCompare(boolean compare) {
        isCompare = compare;
    }

    public String getComparator() {
        return comparator;
    }

    public void setComparator(String comparator) {
        this.comparator = comparator;
    }

    public int getComIntVal() {
        return comIntVal;
    }

    public void setComIntVal(int comIntVal) {
        this.comIntVal = comIntVal;
    }
}
