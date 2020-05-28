package cn.hadoop.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/4/6
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name","local");

        Job job = Job.getInstance(conf,"wc_job");

        //设置运行的jar
        job.setJarByClass(WordCountDriver.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);

        //设置mapper输出格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置启动几个reducer任务运行
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job,new Path("E:\\input"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\output"));

        //提交程序
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
