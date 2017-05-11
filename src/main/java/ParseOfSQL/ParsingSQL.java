package ParseOfSQL;

import domain.BinaryTreeAndOr;
import domain.BinaryTreeAnrOrNode;
import junit.framework.TestCase;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import operation.*;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by yuxiao on 5/11/17.
 * 对一条SQL 语句进行解析
 */
public class ParsingSQL extends TestCase {
    private CCJSqlParserManager parserManager = new CCJSqlParserManager();
    private List<GreaterThan> greaterThanList = new ArrayList<GreaterThan>();
    private List<GreaterThanEquals> greaterThanEqualsList = new ArrayList<GreaterThanEquals>();
    private List<MinorThan> minorThanList = new ArrayList<MinorThan>();
    private List<MinorThanEquals> minorThanEqualsList = new ArrayList<MinorThanEquals>();
    private List<EqualsTo> equalsToList = new ArrayList<EqualsTo>();
    private List<NotEqualsTo> notEqualsToList = new ArrayList<NotEqualsTo>();
    // join 操作，key(String):连接的表名 value(List<String>): 与主表相连的字段
    private HashMap<String, List<String>> joinsMap = new HashMap<String, List<String>>();
    // where 后面表达式生成and 和 or 的二叉树
    private BinaryTreeAndOr mSelectRootExp = null;


    public void testparsingTheSQL() throws Exception {
//        final String statement = "SELECT sku_id,MAX(attr1) as a,attr1,JData_Product.attr2 FROM jingdongdata.JData_Product";
//                final String statement="SELECT * FROM jingdongdata.JData_Product LIMIT 3,1000";

//               final String statement="SELECT sku_id as a,attr1,JData_Product.attr2 FROM jingdongdata.JData_Product";
//                final String statement = "SELECT * FROM jingdongdata.JData_Product where JData_Product.sku_id <= 10000 and attr1>2 LIMIT 1000";

        final String statement = "SELECT * FROM jingdongdata.JData_Product LEFT OUTER JOIN jingdongdata.JData_Comment on JData_Product.sku_id = JData_Comment.sku_id and JData_Product.sku_id = JData_Comment.comment_number INNER JOIN JData_abc on JData_Product.sku_id = JData_abc.y group by JData_Product.sku_id,JData_Product.attr1";

        Select select = (Select) parserManager.parse(new StringReader(statement));
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

        //region 获取 From tableNames
        List<String> fromTables = getFromTables(plainSelect);
        scanList(fromTables);
        String originTable = fromTables.get(0);
        //endregion

        //region 获取映射的属性
//        fun = (Function) ((SelectExpressionItem) plainSelect.getSelectItems().get(1)).
//                getExpression();
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        for (SelectItem item : selectItems) {
            if (item.toString().equals("*")) {  //选择全部
//                System.out.println("*");
                TCItem tcItem = new TCItem();
                tcItem.setColName("*");
                tcItem.setTableName(fromTables.get(0).toString());
            } else {
                SelectExpressionItem sEI = (SelectExpressionItem) item;
                Expression expr = sEI.getExpression();
                if (expr instanceof Column) { //选择某些列，即映射操作
                    Column col = (Column) expr;
                    System.out.println(col.getColumnName() + " : " + col.getFullyQualifiedName());

                    TCItem tcItem = new TCItem();
                    String tTabName = col.getTable().toString();
                    if (tTabName == null || tTabName.isEmpty()) {
                        tTabName = fromTables.get(0).toString();
                    }
                    tcItem.setTableName(tTabName);
                    tcItem.setColName(col.getColumnName());
                    // 加入到本条SQL语句的映射的列集合
                    Projection.proList.add(tcItem);

                } else if (expr instanceof Function) { //聚合操作提取,目前仅支持max 和 count 操作
                    Function fun = (Function) expr;
                    System.out.println(fun.getName());
                    AgregationFunFactor funFactor = new AgregationFunFactor();
                    funFactor.setFunStr(fun.getName()); //提取函数名

                    String tTabName = fromTables.get(0).toString();
                    List<TCItem> tcItemList = new ArrayList<TCItem>();
                    List<Expression> expressionList = fun.getParameters().getExpressions();
                    for (Expression exItem : expressionList) {//提取参数
                        Column paraColumn = (Column) exItem;
                        TCItem tcItem = new TCItem();
                        tcItem.setColName(paraColumn.toString());
                        if (paraColumn.getTable().getName() == null) {
                            tcItem.setTableName(tTabName);
                        } else {
                            tcItem.setTableName(paraColumn.getTable().getName());
                        }
                        tcItemList.add(tcItem);
                    }

                    funFactor.setParameterList(tcItemList);

                    AggregationStream.agreFunList.add(funFactor);

                }

            }
        }
//        assertEquals(3,Projection.proList.size());
        //endregion

        //region 获取 LIMIT
        if (plainSelect.getLimit() != null) {
            Expression offset = plainSelect.getLimit().getOffset();
            Expression rowCount = plainSelect.getLimit().getRowCount();
            long offsetVal = -1;
            long rowCountVal = -1;
            if (isNotNull(offset)) {
                offsetVal = ((LongValue) offset).getValue();
            }
            if (isNotNull(rowCount)) {
                rowCountVal = ((LongValue) rowCount).getValue();
            }
            System.out.println(offsetVal + "," + rowCountVal);
        }

//        assertEquals(3,offsetVal);
//        assertEquals(1000,rowCountVal);
        //endregion


        //region 获取 where, 并把得到的选择语义添加到Selection.binaryTreeAndOr 中
        getWhere(plainSelect);
        if (mSelectRootExp != null) {
            Selection.binaryTreeAndOr = mSelectRootExp;
        }
        assertEquals(mSelectRootExp, Selection.binaryTreeAndOr);
        for (GreaterThan item : greaterThanList) {
            System.out.println(item.getLeftExpression() + "> " + item.getRightExpression());
        }

        for (GreaterThanEquals item : greaterThanEqualsList) {
            System.out.println(item.getLeftExpression() + ">= " + item.getRightExpression());
        }

        for (MinorThan item : minorThanList) {
            System.out.println(item.getLeftExpression() + "< " + item.getRightExpression());
        }
        for (MinorThanEquals item : minorThanEqualsList) {
            System.out.println(item.getLeftExpression() + "<=" + item.getRightExpression());
        }

        for (EqualsTo item : equalsToList) {
            System.out.println(item.getLeftExpression() + "==" + item.getRightExpression());
        }

        for (NotEqualsTo item : notEqualsToList) {
            System.out.println(item.getLeftExpression() + "!=" + item.getRightExpression());
        }
        //endregion

        //region 获取join
        if (!fromTables.isEmpty()){
            JoinCondition.originTabName = fromTables.get(0).toString();
        }
        getJoins(plainSelect);
        //endregion

        //region 获取group
        getGroups(originTable,plainSelect);
        //endregion

        //region 获取having
        List<String> havingExpList = getHaving(plainSelect);
        scanList(havingExpList);
        //endregion
    }


    private void getJoins(PlainSelect plainSelect) {
        List<Join> mJoins = plainSelect.getJoins(); // 注意select * from tab1,tab2 第二个表也为join Item
        if (mJoins != null) {
            for (Join itemJoin : mJoins) {
                String tjoinTableName = ((Table) itemJoin.getRightItem()).getFullyQualifiedName();
                String joinType = ""; // 连接类型目前仅支持 outter join ,inner join , left join
                if (itemJoin.isInner()) {
                    joinType = "Inner";
                } else if (itemJoin.isOuter()) {
                    joinType = "Outer";
                } else {
                    joinType = "Left";
                }
                tjoinTableName += "|" + joinType;
                Expression onExp = itemJoin.getOnExpression();
                if (onExp instanceof AndExpression) {
                    // 解析 tab1.a = tab2.b and tab1.a=tab2.c  默认格式必须为 表名.列名
                    AndExpression tempAndExp = (AndExpression) onExp;
                    EqualsTo righExpEqualsTo = (EqualsTo) tempAndExp.getRightExpression();

                    //保存至JoinConditon
                    saveJoinCondition(tjoinTableName, righExpEqualsTo);

                    Expression leftExp = tempAndExp.getLeftExpression();
                    while (leftExp instanceof AndExpression) {
                        tempAndExp = (AndExpression) leftExp;
                        leftExp = tempAndExp.getLeftExpression();
                    }
                    EqualsTo tEqualsTo = (EqualsTo) leftExp;
                    //保存至JoinConditon
                    saveJoinCondition(tjoinTableName, tEqualsTo);
                } else {
                    EqualsTo tEqualsTo = (EqualsTo) onExp;
                    //保存至JoinConditon
                    saveJoinCondition(tjoinTableName, tEqualsTo);
                }

            }

            assertEquals(2,JoinCondition.linkTablemap.size());
        }
    }

    /**
     * 举例 left join tab2 on tab1.a = tab2.A
     *
     * @param joinTableType   tab2_left
     * @param righExpEqualsTo tab1.a = tab2.A
     */
    public void saveJoinCondition(String joinTableType, EqualsTo righExpEqualsTo) {
        //region 保存至JoinConditon
        Column leftCol = (Column) righExpEqualsTo.getLeftExpression();
        Column rightCol = (Column) righExpEqualsTo.getRightExpression();
        TCItem leftTCItem = new TCItem();
        leftTCItem.setColName(leftCol.getColumnName());
        leftTCItem.setTableName(leftCol.getTable().getName());
        TCItem rightTCItem = new TCItem();
        rightTCItem.setColName(rightCol.getColumnName());
        rightTCItem.setTableName(rightCol.getTable().getName());

        JoinTwoTable joinTwoTable = new JoinTwoTable();
        joinTwoTable.setTcItemLeft(leftTCItem);
        joinTwoTable.setTcItemRight(rightTCItem);
        JoinCondition.linkTablemap.put(joinTableType, joinTwoTable);
        //endregion
    }


    /**
     * 解析group by的语义，并保存到GoupBy 中
     * @param defalutTable 默认的表名
     * @param plainSelect SQL
     *
     */
    private void getGroups(String defalutTable, PlainSelect plainSelect) {
        List<Expression> groupExps = plainSelect.getGroupByColumnReferences();
        if (groupExps != null) {
            for (Expression item : groupExps) {
                Column column = (Column)item;
                TCItem tcItem = new TCItem();
                tcItem.setColName(column.getColumnName());
                if (column.getTable().getName()==null ||column.getTable().getName().isEmpty() ){
                    tcItem.setTableName(defalutTable);
                }else{
                    tcItem.setTableName(column.getTable().getName());
                }
                GroupBy.groupList.add(tcItem);
            }
        }
//        assertEquals(2,GroupBy.groupList.size());
    }

    // 返回from 的表名集合
    private List<String> getFromTables(PlainSelect plainSelect) {
        List<String> results = new ArrayList<String>();
        List<Join> joins = plainSelect.getJoins();
        FromItem fromItem = plainSelect.getFromItem();
        System.out.println(fromItem.toString());
//        System.out.println(fromItem.getAlias().getName());
        results.add(fromItem.toString());
        if (joins != null) {
            for (Join join : joins) {
//            System.out.println(join.getRightItem().getAlias().getName());
                results.add(join.getRightItem().toString());
            }
        }

        return results;
    }

    private void explain(Expression rightExp) {
        if (rightExp instanceof GreaterThan) {
            greaterThanList.add((GreaterThan) rightExp);

        } else if (rightExp instanceof GreaterThanEquals) {
            greaterThanEqualsList.add((GreaterThanEquals) rightExp);

        } else if (rightExp instanceof MinorThan) {
            minorThanList.add((MinorThan) rightExp);

        } else if (rightExp instanceof MinorThanEquals) {
            minorThanEqualsList.add((MinorThanEquals) rightExp);
        } else if (rightExp instanceof NotEqualsTo) {
            notEqualsToList.add((NotEqualsTo) rightExp);
        } else if (rightExp instanceof EqualsTo) {
            equalsToList.add((EqualsTo) rightExp);
        }
    }

    //递归解析 and 和 or 混合表达式,并生成相关的选择表达式树
    private void solveAndOr(Expression exp, boolean flag, BinaryTreeAnrOrNode root) {
        if (!flag) {
            root.isLeave = true;
            root.setmExp(exp);
            explain(exp);
        } else {
            if (exp instanceof AndExpression) {
                AndExpression newexp = (AndExpression) exp;
                root.isAndExp = true;
                root.left = new BinaryTreeAnrOrNode();
                root.right = new BinaryTreeAnrOrNode();
                solveAndOr(newexp.getLeftExpression(), true, root.left);
                solveAndOr(newexp.getRightExpression(), true, root.right);
            } else if (exp instanceof OrExpression) {
                OrExpression newexp = (OrExpression) exp;
                root.isOrExp = true;
                root.left = new BinaryTreeAnrOrNode();
                root.right = new BinaryTreeAnrOrNode();
                solveAndOr(newexp.getLeftExpression(), true, root.left);
                solveAndOr(newexp.getRightExpression(), true, root.right);
            } else {

                solveAndOr(exp, false, root);
            }
        }
    }

    private void getWhere(PlainSelect plainSelect) {
        Expression whereExp = plainSelect.getWhere();
        if (whereExp != null) {
            String whereStr = whereExp.toString();
            System.out.println(whereStr);
            mSelectRootExp = new BinaryTreeAndOr();
            mSelectRootExp.root = new BinaryTreeAnrOrNode();
            solveAndOr(whereExp, true, mSelectRootExp.root);

        }

    }

    //返回 having 后的聚集操作，目前只考虑仅有一个having条件 即 having a.b >1
    private List<String> getHaving(PlainSelect plainSelect) {
        List<String> havingList = new ArrayList<String>();
        Expression havExp = plainSelect.getHaving();
        if (havExp != null) {
            String havingStr = havExp.toString();
            System.out.println(havingStr);
            String ansStr = "";
            if (havExp instanceof GreaterThan) {
                GreaterThan gt = (GreaterThan) havExp;
                ansStr = gt.getLeftExpression().toString() + ">" + gt.getRightExpression().toString();
            } else if (havExp instanceof GreaterThanEquals) {
                GreaterThanEquals gte = (GreaterThanEquals) havExp;
                ansStr = gte.getLeftExpression().toString() + ">=" + gte.getRightExpression().toString();

            } else if (havExp instanceof MinorThan) {
                MinorThan mt = (MinorThan) havExp;
                ansStr = mt.getLeftExpression().toString() + "<" + mt.getRightExpression().toString();

            } else if (havExp instanceof MinorThanEquals) {
                MinorThanEquals mte = (MinorThanEquals) havExp;
                ansStr = mte.getLeftExpression().toString() + "<=" + mte.getRightExpression().toString();
            } else if (havExp instanceof NotEqualsTo) {
                NotEqualsTo net = (NotEqualsTo) havExp;
                ansStr = net.getLeftExpression().toString() + "!=" + net.getRightExpression().toString();
            } else if (havExp instanceof EqualsTo) {
                EqualsTo et = (EqualsTo) havExp;
                ansStr = et.getLeftExpression().toString() + "==" + et.getRightExpression().toString();
            }
            havingList.add(ansStr);
        }
        return havingList;
    }

    private boolean isNotNull(Expression exr) {
        return exr != null;
    }

    private void scanList(List<String> target) {
        System.out.println("\n----------------------");
        for (String item : target) {
            System.out.println(item);
        }
        System.out.println("********************\n");

    }
}
