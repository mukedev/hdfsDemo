package cn.hadoop.mapreduce.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author zhangYu
 * @date 2020/4/6
 */
public class MyMapper extends Mapper<LongWritable, Text, IntPair, IntWritable> {
    private final IntPair key = new IntPair();
    private final IntWritable value = new IntWritable();

    @Override
    protected void map(LongWritable inKey, Text inValue, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(inValue.toString());
        int left = 0;
        int right = 0;
        if (itr.hasMoreTokens()) {
            left = Integer.parseInt(itr.nextToken());
            if (itr.hasMoreTokens()) {
                right = Integer.parseInt(itr.nextToken());
            }
            key.setFirst(left);
            key.setSecond(right);
            value.set(right);
            context.write(key,value);
        }
    }
}
