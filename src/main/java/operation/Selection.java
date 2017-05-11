package operation;

import domain.BinaryTreeAndOr;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yuxiao on 5/11/17.
 * 封装select tab1.a,tabl.b,tab2.A from tab1,tab2 where tab1.a>1000 and tabl.b<22语句中的选择操作
 */
public class Selection {
    public static List<String> fromTableList = new ArrayList<String>(); // from 后面的表名list
    public static BinaryTreeAndOr binaryTreeAndOr = null; // and 和 or 表达式组成的二叉树

}
