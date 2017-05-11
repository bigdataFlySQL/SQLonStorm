package operation;

/**
 * Created by yuxiao on 5/11/17.
 * 封装select tab1.a,tabl.b,tab2.A from tab1 outer join tab2 on tab1.a=tab2.A 语句中的tab1.a=tab2.A连接操作,默认隐含" = "相等的判断
 */
public class JoinTwoTable {
    private TCItem tcItemLeft;
    private  TCItem tcItemRight;

    public TCItem getTcItemLeft() {
        return tcItemLeft;
    }

    public void setTcItemLeft(TCItem tcItemLeft) {
        this.tcItemLeft = tcItemLeft;
    }

    public TCItem getTcItemRight() {
        return tcItemRight;
    }

    public void setTcItemRight(TCItem tcItemRight) {
        this.tcItemRight = tcItemRight;
    }
}
