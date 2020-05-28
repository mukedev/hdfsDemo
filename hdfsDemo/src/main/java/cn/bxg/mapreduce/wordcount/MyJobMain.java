package cn.bxg.mapreduce.wordcount;

import cn.hadoop.mapreduce.wc.MyReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author zhangYu
 * @date 2020/5/24
 */
public class MyJobMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        String inPath = "hdfs://had-node:8020/wc";
        String outPath = "hdfs://had-node:8020/out_wc";

        FileSystem fileSystem = FileSystem.get(new URI(inPath), new Configuration());
        if (fileSystem.exists(new Path(outPath))){
            fileSystem.delete(new Path(outPath),true);
            //fileSystem.close();
        }

        //获取job类
        Job job = Job.getInstance(new Configuration(), "wc_test");
        job.setJarByClass(MyJobMain.class);

        /**
         * mapReduce八个步骤
         */
        //1.配置文件路径和读取方式
        FileInputFormat.addInputPath(job,new Path(inPath));
        job.setInputFormatClass(TextInputFormat.class);
        //2.配置mapper类和输出key-value数据类型
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //3.分区
        //4.排序
        //5.规约
        //6.分组
        //7.配置reduce类和输出key-value数据类型
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //8.配置文件输出方式
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,new Path(outPath));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
