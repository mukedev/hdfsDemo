package cn.bxg.mapreduce.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/5/31
 */
public class SortBean implements WritableComparable<SortBean> {

    private String name;
    private Integer num;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return name + "\t" + num;
    }

    @Override
    public int compareTo(SortBean o) {
        int result = this.getName().compareTo(o.getName());
        if (result == 0) {
            return this.num - this.getNum();
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeInt(num);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.num = dataInput.readInt();
    }
}
