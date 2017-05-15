package definetable;

/**
 * Created by yao on 5/10/17.
 */
public class MField {
    private String name;
    private String default_value;
    private String type;

    /**
     * setField函数，列的设置函数
     * @param name 列名
     * @param default_value 列中元素默认值
     * @param type 列中元素的类型
     */
    public void setField(String name, String default_value,String type){
        this.name = name;
        this.default_value = default_value;
        this.type = type;
    }

    /**
     * getField函数，读取域信息的函数
     * @return 返回一个String类型的域信息
     */
    public String[] getField(){
        String []field = new String[3];
        field[0] = this.name;
        field[1] = this.default_value;
        field[2] = this.type;
        return field;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDefault_value() {
        return default_value;
    }

    public void setDefault_value(String default_value) {
        this.default_value = default_value;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
