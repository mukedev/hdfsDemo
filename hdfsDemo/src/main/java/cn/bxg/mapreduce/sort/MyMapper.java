package cn.bxg.mapreduce.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/5/31
 */
public class MyMapper extends Mapper<LongWritable, Text, SortBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        SortBean sortBean = new SortBean();
        String[] line = value.toString().split("\t");
        for (int i = 0; i < line.length; i++) {
            sortBean.setName(line[0]);
            sortBean.setNum(Integer.parseInt(line[1]));
            context.write(sortBean, NullWritable.get());
        }
    }
}
