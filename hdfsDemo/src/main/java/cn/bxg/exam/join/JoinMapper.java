package cn.bxg.exam.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/6/29
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        if (fileName.equals("visit.log")){
            String[] split = value.toString().split(",");
            String vIp = split[0];
            context.write(new Text(vIp),value);
        } else {
            String[] split = value.toString().split(",");
            String vIp = split[1];
            context.write(new Text(vIp),value);
        }

    }
}
