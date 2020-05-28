package cn.hadoop.mapreduce.secondarysort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/4/6
 */
public class IntPair implements WritableComparable {

    private int first;
    private int second;

    public IntPair(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public IntPair(){}

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public int compareTo(Object o) {
        IntPair pair = (IntPair)o;
        if (first != pair.first) {
            return first - pair.first;
        } else if (second != pair.second) {
            return second - pair.second;
        }
        return 0;
    }

    /**
     * 序列化
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(first);
        dataOutput.writeInt(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readInt();
        this.second = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "IntPair{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }
}
