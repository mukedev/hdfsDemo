package cn.bxg.mapreduce.sort2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zhangYu
 * @program hdfsDemo
 * @description sortBean
 * @create 2020-06-01 09:23
 **/
public class SortBean implements WritableComparable<SortBean> {

    private String word;
    private Integer num;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return word + "\t" + num + "\n";
    }

    @Override
    public int compareTo(SortBean o) {
        int result = this.getWord().compareTo(o.getWord());
        if (result == 0) {
            return this.num - o.getNum();
        }
        return result;
    }

    /**
     * 序列化
     *
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        dataOutput.writeInt(num);
    }

    /**
     * 反序列化
     *
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word = dataInput.readUTF();
        this.num = dataInput.readInt();
    }
}
