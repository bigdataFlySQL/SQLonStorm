package operation;

import java.util.List;

/**
 * Created by yuxiao on 5/11/17.
 * 封装select tab1.a,MAX(tab1.c) from tab1having tab1.d>111语句中的聚合操作算子 如MAX(tab1.c)
 *
 */
public class AgregationFunFactor {

    private String funStr; // 聚合函数名,目前仅支持 Max, count
    private List<TCItem> parameterList; // 函数列表 如MAX(tab1.c) 则list集合为{tab1.c}

    public String getFunStr() {
        return funStr;
    }

    public void setFunStr(String funStr) {
        this.funStr = funStr;
    }

    public List<TCItem> getParameterList() {
        return parameterList;
    }

    public void setParameterList(List<TCItem> parameterList) {
        this.parameterList = parameterList;
    }
}
