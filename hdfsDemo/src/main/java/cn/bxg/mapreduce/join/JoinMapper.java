package cn.bxg.mapreduce.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author zhangYu
 * @program hdfsDemo
 * @description
 * @create 2020-06-11 11:06
 **/
public class JoinMapper extends Mapper<LongWritable, Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //判断数据来自哪个文件
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        String[] line = value.toString().split(",");

        //商品表
        if (fileName.startsWith("product.txt")){
            context.write(new Text(line[0]),value);
        } else {
         //订单表
            context.write(new Text(line[2]),value);
        }
    }
}
