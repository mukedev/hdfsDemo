package cn.bxg.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/5/23
 *
 * k1       v1
 * 0        hadoop hive hadoop spark
 */
public class MyMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    private Text k2 = new Text();
    private IntWritable v2 = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");

        for (int i = 0; i < split.length; i++) {
            k2.set(split[i]);
            context.write(k2,v2);
        }
    }
}
