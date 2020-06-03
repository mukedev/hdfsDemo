package cn.bxg.mapreduce.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * @description combiner
 * @create 2020-06-01 10:57
 **/
public class CombinerApp {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        String inPath = "hdfs://had-node:8020/combiner/input";
        String outPath = "hdfs://had-node:8020/combiner/output";

        //获取job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(CombinerApp.class);

        //判断输出文件是否存在
        FileSystem fileSystem = FileSystem.get(new URI(inPath), conf);
        if (fileSystem.exists(new Path(outPath))){
            fileSystem.delete(new Path(outPath),true);
        }

        /**
         * 八大步骤
         */
        //1.配置输入路径和读取文件格式
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(inPath));
        //2.配置mapper主类和数据输出类型
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //3.分区；4.排序；
        //5.规约
        job.setCombinerClass(MyCombiner.class);
        //6.分组
        //7.配置reduce的主类和数据输出类型
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //8.配置输出路径和输出文件格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(outPath));

        //退出
        System.exit(job.waitForCompletion(true)?0:1);
    }
}


