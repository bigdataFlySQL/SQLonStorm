package operation;

import java.util.HashMap;

/**
 * Created by yuxiao on 5/11/17.
 *  封装select tab1.a,tabl.b,tab2.A from tab1 outer join tab2 on tab1.a=tab2.A 语句中的连接操作
 */
public class JoinCondition {
    public static String originTabName = null; //左边的表名
    // 右边的表，即要连接的表和连接的字段 key: 表名_连接类型  如tab2_outer join  value:连接的字段JoinTwoTable
    public static HashMap<String,JoinTwoTable> linkTablemap=new HashMap<String, JoinTwoTable>();
}
