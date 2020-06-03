package cn.bxg.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhangYu
 * @program hdfsDemo
 * @description mapper
 * @create 2020-06-01 10:58
 **/
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    IntWritable val = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        for (int i = 0; i < line.length; i++) {
            context.write(new Text(line[i]), val);
        }
    }
}
