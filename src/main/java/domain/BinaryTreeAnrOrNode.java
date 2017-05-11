package domain;

import net.sf.jsqlparser.expression.Expression;
import operation.TCItem;

/**
 * Created by yuxiao on 5/10/17.
 * 封装 选择操作元素的二叉树的节点，叶子节点为对应的选择表达式，非叶子节点为对应的and , or 操作
 */
public class BinaryTreeAnrOrNode {
    private Expression mExp;// 记录叶子节点的表达式
    private TCItem mcItem; // 将叶子节点的表达式封装成TCItem
    public BinaryTreeAnrOrNode left;
    public BinaryTreeAnrOrNode right;

    public boolean isAndExp; //and
    public boolean isOrExp;  // or
    public boolean isLeave; // 是否是叶子节点

    public TCItem getMcItem() {
        return mcItem;
    }

    public void setMcItem(TCItem mcItem) {
        this.mcItem = mcItem;
    }

    public Expression getmExp() {
        return mExp;
    }

    public void setmExp(Expression mExp) {
        this.mExp = mExp;
    }
}
