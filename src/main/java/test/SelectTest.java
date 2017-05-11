package test;

import domain.BinaryTreeAndOr;
import domain.BinaryTreeAnrOrNode;
import junit.framework.TestCase;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;


import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SelectTest extends TestCase {
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
    private BinaryTreeAndOr mSeleectRootExp= null;

    public SelectTest(String arg0) {
        super(arg0);
    }

    public void testColumnName() throws Exception {
//        final String statement="SELECT sku_id as a,attr1,JData_Product.attr2 FROM jingdongdata.JData_Product";
//        final String statement="SELECT * FROM jingdongdata.JData_Product LIMIT 3,1000";
//        final String statement="SELECT * FROM jingdongdata.JData_Product LIMIT 1000";
//        final String statement = "SELECT * FROM jingdongdata.JData_Product,JData_Action_0415  where JData_Product.sku_id <= 10000 and attr1>2 LIMIT 1000";
//        final String statement = "SELECT * FROM jingdongdata.JData_Product LEFT OUTER JOIN jingdongdata.JData_Comment on JData_Product.sku_id = JData_Comment.sku_id and JData_Product.sku_id = JData_Comment.comment_number INNER JOIN JData_abc on JData_Product.sku_id = JData_abc.y";

//        final String statement = "SELECT * FROM jingdongdata.JData_Product LEFT OUTER JOIN jingdongdata.JData_Comment on JData_Product.sku_id = JData_Comment.sku_id and JData_Product.sku_id = JData_Comment.comment_number and JData_Product.sku_id = JData_Comment.b and JData_Product.sku_id=Data_Comment.c INNER JOIN JData_abc on JData_Product.sku_id = JData_abc.y group by JData_Product.sku_id,JData_Product.attr1 having JData_Product.sku_id >10000";

        final String statement = "select *,Max(tab1.a),Count(*) as thecount,tab1.d from tab1 where tab1.a=1000 or tab1.b>10 and tab1.c<13";

        Select select = (Select) parserManager.parse(new StringReader(statement));
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

        //region 获取映射的属性
//        fun = (Function) ((SelectExpressionItem) plainSelect.getSelectItems().get(1)).
//                getExpression();
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        for (SelectItem item : selectItems) {
            if (item.toString().equals("*")) {  //选择全部
                System.out.println("*");
            } else {
                SelectExpressionItem sEI = (SelectExpressionItem) item;
                Expression expr = sEI.getExpression();
                if (expr instanceof  Column){ //选择某些列
                    Column col = (Column) expr;
                    System.out.println(col.getColumnName() + " : " + col.getFullyQualifiedName());
                }else if(expr instanceof Function){ //聚合操作
                    Function fun = (Function)expr;
                    System.out.println(fun.getName());
                    System.out.println(fun.getParameters());
                    System.out.println(fun.getAttribute());
                }

            }
        }
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


        //region 获取 From
        List<String> fromTables = getFromTables(plainSelect);
        scanList(fromTables);
        //endregion

        //region 获取 where
        getWhere(plainSelect);
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
        getJoins(plainSelect);
        //endregion

        //region 获取group
        List<String> groupsExpStrList = getGroups(plainSelect);
        scanList(groupsExpStrList);
        //region

        //region 获取having
        List<String> havingExpList = getHaving(plainSelect);
        scanList(havingExpList);
        //endregion
    }

    private void getJoins(PlainSelect plainSelect) {
        List<Join> mJoins = plainSelect.getJoins();
        if (mJoins != null) {
            for (Join itemJoin : mJoins) {
                String tjoinTableName = ((Table) itemJoin.getRightItem()).getFullyQualifiedName();
                Expression onExp = itemJoin.getOnExpression();
                List<String> tValList = new ArrayList<String>();
                if (onExp instanceof AndExpression) {
                    AndExpression tempAndExp = (AndExpression) onExp;
                    tValList.add(tempAndExp.getRightExpression().toString());
                    Expression leftExp = tempAndExp.getLeftExpression();
                    while (leftExp instanceof AndExpression) {
                        tempAndExp = (AndExpression) leftExp;
                        tValList.add(tempAndExp.getRightExpression().toString());
                        leftExp = tempAndExp.getLeftExpression();
                    }
                    tValList.add(leftExp.toString());
                } else {
                    tValList.add(onExp.toString());
                }
                joinsMap.put(tjoinTableName, tValList);
                scanList(tValList);
            }
        }
    }

    // 返回group 的列名集合
    private List<String> getGroups(PlainSelect plainSelect) {
        List<String> groupStrs = new ArrayList<String>();

        List<Expression> groupExps = plainSelect.getGroupByColumnReferences();
        if (groupExps != null) {
            for (Expression item : groupExps) {
                groupStrs.add(item.toString());
            }
        }
        return groupStrs;
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
            root.isLeave=true;
            root.setmExp(exp);
            explain(exp);
        } else {
            if (exp instanceof AndExpression) {
                AndExpression newexp = (AndExpression) exp;
                root.isAndExp = true;
                root.left = new BinaryTreeAnrOrNode();
                root.right = new BinaryTreeAnrOrNode();
                solveAndOr(newexp.getLeftExpression(), true,root.left);
                solveAndOr(newexp.getRightExpression(), true,root.right);
            } else if (exp instanceof OrExpression) {
                OrExpression newexp = (OrExpression) exp;
                root.isOrExp = true;
                root.left = new BinaryTreeAnrOrNode();
                root.right = new BinaryTreeAnrOrNode();
                solveAndOr(newexp.getLeftExpression(), true,root.left);
                solveAndOr(newexp.getRightExpression(), true,root.right);
            } else {

                solveAndOr(exp, false,root);
            }
        }
    }

    private void getWhere(PlainSelect plainSelect) {
        Expression whereExp = plainSelect.getWhere();
        if (whereExp != null) {
            String whereStr = whereExp.toString();
            System.out.println(whereStr);
            mSeleectRootExp = new BinaryTreeAndOr();
            mSeleectRootExp.root = new BinaryTreeAnrOrNode();
            solveAndOr(whereExp, true,mSeleectRootExp.root);

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