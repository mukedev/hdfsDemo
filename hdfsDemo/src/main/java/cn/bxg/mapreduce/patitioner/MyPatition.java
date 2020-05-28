package cn.bxg.mapreduce.patitioner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author zhangYu
 * @date 2020/5/25
 */
public class MyPatition extends Partitioner<Text, NullWritable> {

    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        String[] split = text.toString().split("\t");
        if (Integer.parseInt(split[5]) > 15) {
            return 1;
        }
        return 0;
    }
}
