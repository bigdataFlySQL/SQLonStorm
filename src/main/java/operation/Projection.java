package operation;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yuxiao on 5/11/17.
 *  封装select tab1.a,tabl.b,tab2.A limit 1,20语句中的映射
 */
public class Projection {
    public static List<TCItem> proList= new ArrayList<TCItem>(); //所要映射元素的集合
    public static int limitStart=1; //SQL 语句中的limit 选择限制
    public static int limitEnd =1000;

}
