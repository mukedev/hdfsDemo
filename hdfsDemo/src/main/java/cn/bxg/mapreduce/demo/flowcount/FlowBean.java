package cn.bxg.mapreduce.demo.flowcount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/6/3
 */
public class FlowBean implements Writable {
    private long upPackNum;
    private long downPackNum;
    private long upPayLoad;
    private long downPayLoad;

    public long getUpPackNum() {
        return upPackNum;
    }

    public void setUpPackNum(long upPackNum) {
        this.upPackNum = upPackNum;
    }

    public long getDownPackNum() {
        return downPackNum;
    }

    public void setDownPackNum(long downPackNum) {
        this.downPackNum = downPackNum;
    }

    public long getUpPayLoad() {
        return upPayLoad;
    }

    public void setUpPayLoad(long upPayLoad) {
        this.upPayLoad = upPayLoad;
    }

    public long getDownPayLoad() {
        return downPayLoad;
    }

    public void setDownPayLoad(long downPayLoad) {
        this.downPayLoad = downPayLoad;
    }

    @Override
    public String toString() {
        return upPackNum +
                "\t" + downPackNum +
                "\t" + upPayLoad +
                "\t" + downPayLoad;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upPackNum);
        dataOutput.writeLong(downPackNum);
        dataOutput.writeLong(upPayLoad);
        dataOutput.writeLong(downPayLoad);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upPackNum = dataInput.readLong();
        this.downPackNum = dataInput.readLong();
        this.upPayLoad = dataInput.readLong();
        this.downPayLoad = dataInput.readLong();
    }
}
