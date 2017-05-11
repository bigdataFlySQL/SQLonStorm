package domain;

import net.sf.jsqlparser.expression.Expression;

/**
 * Created by yuxiao on 5/10/17.
 * 封装 选择操作元素的二叉树的节点，叶子节点为对应的选择表达式，非叶子节点为对应的and , or 操作
 */
public class BinaryTreeAnrOrNode {
    private Expression mExp;
    public BinaryTreeAnrOrNode left;
    public BinaryTreeAnrOrNode right;

    public boolean isAndExp;
    public boolean isOrExp;
    public boolean isLeave;

    public Expression getmExp() {
        return mExp;
    }

    public void setmExp(Expression mExp) {
        this.mExp = mExp;
    }
}
