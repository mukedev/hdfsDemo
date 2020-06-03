package cn.bxg.mapreduce.sort2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhangYu
 * @program hdfsDemo
 * @description mapper
 * @create 2020-06-01 09:22
 **/
public class MyMapper extends Mapper<LongWritable, Text, SortBean, NullWritable> {
    SortBean sortBean = new SortBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        sortBean.setWord(line[0]);
        sortBean.setNum(Integer.parseInt(line[1]));
        context.write(sortBean, NullWritable.get());
    }
}
