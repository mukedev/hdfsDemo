package cn.hadoop.mapreduce.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
public class SecondSortDriver {
    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
        String intputPath = "/insecondsort";
        String outputPath = "/outsecondsort";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(intputPath), conf);
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath));
        }

        Job job = Job.getInstance(conf, "SecondSort");

        //设置运行主类
        job.setJarByClass(SecondSortDriver.class);

        //设置Mapper
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        //分组函数
        job.setGroupingComparatorClass(GroupingComparator.class);

        //设置Reducer
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job,intputPath);
        FileOutputFormat.setOutputPath(job,new Path(outputPath));


        System.exit(job.waitForCompletion(true)?0:1);

    }
}
