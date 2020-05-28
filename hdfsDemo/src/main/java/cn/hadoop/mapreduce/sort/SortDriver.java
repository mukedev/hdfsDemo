package cn.hadoop.mapreduce.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author zhangYu
 * @date 2020/4/6
 */
public class SortDriver {

    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
        String inputPath = "/sort";
        String outputPath = "/outputsort";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(inputPath), conf);

        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath),true);
        }

        Job job = Job.getInstance(conf, "SortDriver");

        //设置运行主类
        job.setJarByClass(SortDriver.class);

        //设置mapper
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reducer
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,new Path(outputPath));

        System.exit(job.waitForCompletion(true)?0:1);


    }
}
