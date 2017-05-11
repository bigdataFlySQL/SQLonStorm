package operation;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yuxiao on 5/11/17.
 *  封装select tab1.a,tabl from tab1 group by  tab1.c语句中的group by 操作
 */
public class GroupBy {
    public static List<TCItem> groupList = new ArrayList<TCItem>(); // 要group by 的元素集合 如tab1.c,tab1.d
}
