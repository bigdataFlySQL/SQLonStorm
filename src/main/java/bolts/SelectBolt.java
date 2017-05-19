package bolts;

import domain.BinaryTreeAndOr;
import domain.BinaryTreeAnrOrNode;
import operation.Selection;
import operation.TCItem;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.*;

/**
 * 根据 SQL语句 中的选择语义，处理spout 流过来的tuple
 */
public class SelectBolt extends BaseBasicBolt {

    // 记录上一个spout 传过来的tuple 属性名列表,第0个元素为表名
    private List<String> inputvalueNameList;
    // 记录选择操作中间处理结果
    private List<String> results;

    private List<String> descOfOutputFileds;

    public List<String> getDescOfOutputFileds() {
        return descOfOutputFileds;
    }

    public void setDescOfOutputFileds(List<String> descOfOutputFileds) {
        this.descOfOutputFileds = descOfOutputFileds;
    }

    public void prepare(Map stormConf, TopologyContext context) {

        results = new ArrayList<>();

//        // 获取StreamSataReaderSpout 传过来的每一行数据的属性信息 如 user_id,sku_id,cate
//        Map<String, Map<String, List<String>>> inputFields = context.getThisInputFields();
//        Iterator<String> iter = inputFields.keySet().iterator();
//        while (iter.hasNext()) {
//            String key = iter.next();
//            Map<String, List<String>> val = inputFields.get(key);
//            Iterator<String> iter2 = val.keySet().iterator();
//            while (iter2.hasNext()) {
//                inputvalueNameList = val.get(iter2.next());
//                for (String item : inputvalueNameList) {
//                    System.out.println(item);
//                }
//            }

//        }?
    }


    public Map<String, Object> getComponentConfiguration() {
        return super.getComponentConfiguration();
    }

    public void cleanup() {
        System.out.println("选择打印结果");
        for (String item : results) {
            System.out.println(item);
        }
    }

    /**
     * 执行SQL 选择操作
     */
    public void execute(Tuple input, BasicOutputCollector collector) {

        BinaryTreeAndOr bTreeAndOr = Selection.binaryTreeAndOr;
        if (bTreeAndOr != null) {
            postorderTraversal(bTreeAndOr.root, input);

            if (bTreeAndOr.root.val) {
                // 该条tuple符合选择需求
                String ansStr = "";
                for (int i = 0; i < input.size(); i++) {
                    ansStr += input.getString(i) + ",";
                }
                results.add(ansStr);

                collector.emit(input.getValues());
            }

        } else {
            // 无SQL 选择操作需求，tuple直接流入下一个bolt
            int fc = input.getValues().size();
            collector.emit(input.getValues());
        }
    }

    /**
     * 后序遍历选择and 和 or 二叉树，得出一条数据的选择过滤结果 true 或者false
     *
     * @param root 根节点
     */
    public void postorderTraversal(BinaryTreeAnrOrNode root, Tuple tuple) {

        // 获取这次tuple的表名
        String curTableName = tuple.getStringByField("Table");
        if (curTableName.equals("JData_Action_201602")){
            System.out.println("SelectBolt");
        }

        Stack<BinaryTreeAnrOrNode> stack = new Stack<BinaryTreeAnrOrNode>();
        BinaryTreeAnrOrNode p, r;
        r = null;
        p = root;
        while (p != null || !stack.isEmpty()) {
            if (p != null) {
                stack.push(p);
                p = p.left;
            } else {
                p = stack.peek();
                if (p.right != null && p.right != r) {
                    p = p.right;
                    stack.push(p);
                    p = p.left;
                } else {
                    p = stack.pop();
                    if (!p.isLeave) {
                        // 非叶子节点
                        p.val = (p.isAndExp) ? (p.left.val && p.right.val) : (p.left.val || p.right.val);
                    } else {
                        // 叶子节点
                        p.val = false;
                        TCItem tcItem = p.getMcItem();
                        if (curTableName.equals(tcItem.getTableName())) {
                            //要比较的表名相同
                            // 从当前tuple中获取表达式左边待比较的值
                            Integer compLeftVal = Integer.valueOf((String) tuple.getValueByField(tcItem.getColName()));
                            Integer compRightVal = tcItem.getComIntVal();
                            // 获取比较符号 >,<,=,!=
                            String compOperator = tcItem.getComparator();
                            if (compOperator.equals(">")) {
                                if (compLeftVal > compRightVal) {
                                    p.val = true;
                                }
                            } else if (compOperator.equals("<")) {
                                if (compLeftVal < compRightVal) {
                                    p.val = true;
                                }

                            } else if (compOperator.equals("=")) {
                                if (compLeftVal.equals(compRightVal) ) {
                                    p.val = true;
                                }
                            } else if (compOperator.equals("!=")) {
                                if (!compLeftVal.equals(compRightVal)) {
                                    p.val = true;
                                }
                            }
                        }

                    }
                    r = p;
                    p = null;
                }
            }
        }
    }


    /**
     * 输出元组数据属性名到下一个bolt
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (this.descOfOutputFileds == null){
            System.out.print("we");

        }
        Fields fields = new Fields(this.descOfOutputFileds);

        declarer.declare(fields);
    }

}
