package cn.bxg.exam.top;


import cn.bxg.exam.join.JoinMapper;
import cn.bxg.exam.join.JoinReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author zhangYu
 * @program hdfsDemo
 * @description
 * @create 2020-06-29 10:45
 **/
public class TopExamApp {
    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
        String inPath = "hdfs://node01:8020/exam/joinOutput/";
        String outPath = "hdfs://node01:8020/exam/topOutput";

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(inPath),conf);
        if (fileSystem.exists(new Path(outPath))){
            fileSystem.delete(new Path(outPath), true);
        }

        Job job = Job.getInstance(conf,"exam_top");
        job.setJarByClass(TopExamApp.class);

        /**
         * 八大步骤
         */
        //1.设置文件输入路径和输入格式
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(inPath));
        //2.设置mapper类和key-value输出类型
        job.setMapperClass(TopMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //3.分区
        //4.排序
        //5.规约
        //6.分组
        //7.设置reduce主类和key-value输出类型
        job.setReducerClass(TopReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //8.设置结果输出路径和输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(outPath));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
