package cn.bxg.mapreduce.patitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
 * @date 2020/5/25
 */
public class JobMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //文件路径
        String inPath = "hdfs://had-node:8020/partition/input";
        String outPath = "hdfs://had-node:8020/partition/output";

        FileSystem fileSystem = FileSystem.get(new URI(inPath), new Configuration());
        if (fileSystem.exists(new Path(outPath))){
            fileSystem.delete(new Path(outPath),true);
        }

        //获取job实例并设置主类
        Job job = Job.getInstance(new Configuration(), "partition_test");
        job.setJarByClass(JobMain.class);

        /**
         * MapReduce八个步骤
         */
        //1.配置文件路径和读取方式
        FileInputFormat.addInputPath(job,new Path(inPath));
        job.setInputFormatClass(TextInputFormat.class);
        //2.配置mapper主类和数据类型
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //shuff阶段
        //3.分区
        job.setPartitionerClass(MyPatition.class);
        job.setNumReduceTasks(2);
        //4.排序
        //5.规约
        //6.分组
        //7.配置reduce主类和数据类型
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //8.配置文件的输出路径和输出方式
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,new Path(outPath));

        //等待程序结束
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
