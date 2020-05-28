package cn.hadoop.mapreduce.recordreader;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author zhangYu
 * @date 2020/4/6
 */
public class MyDriver {

    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
        String inputPath = "/recordreader";
        String outputPath = "/outputrecordreader";

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI(inputPath), conf);
        if (fs.exists(new Path(outputPath))){
            fs.delete(new Path(outputPath));
        }

        Job job = Job.getInstance(conf, "RecordReaderApp");

        //运行jar类
        job.setJarByClass(MyDriver.class);

        //设置输入和输入数据格式化类
        FileInputFormat.setInputPaths(job,inputPath);
        job.setInputFormatClass(MyInputFormat.class);

        //设置自定义mapper类和map函数输出数据的key和value的类型
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置分区和reduce数量
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(2);

        //Shuffle把数据从Map端拷贝到Reduce端
        //设置自定义reducer类和输出数据的key和value的类型
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        //设置输出目录
        FileOutputFormat.setOutputPath(job,new Path(outputPath));
        job.setOutputFormatClass(TextOutputFormat.class);

        //提交作业
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
