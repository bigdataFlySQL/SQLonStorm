package operation;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yuxiao on 5/11/17.
 * 封装select tab1.a,MAX(tab1.c) from tab1 group by  tab1.c having tab1.d>111语句中的聚合操作
 */
public class AggregationStream {
    public static List<TCItem> havingList = new ArrayList<TCItem>(); // having 后面的选择条件集合
    public static List<AgregationFunFactor> agreFunList= new ArrayList<AgregationFunFactor>(); // select 与where之间后面聚合操作的集合
}
