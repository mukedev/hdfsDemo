package cn.hadoop.mapreduce.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/4/6
 */
public class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private IntWritable data = new IntWritable(1);

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable val : values) {
            context.write(data, key);
            data = new IntWritable(data.get() + 1);
        }

    }
}
