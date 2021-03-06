package cn.bxg.exam.top;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * @author zhangYu
 * @date 2020/6/29
 */
public class TopReduce extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (Text val: values) {
            i++;
        }
        context.write(key,new IntWritable(i));
    }
}

