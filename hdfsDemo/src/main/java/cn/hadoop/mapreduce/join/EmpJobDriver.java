package cn.hadoop.mapreduce.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 驱动类
 * @author zhangYu
 * @date 2020/4/6
 */
public class EmpJobDriver {

    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
        String inputPath = "/inputjoin";
        String outputPath = "/outputjoin";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(inputPath), conf);
        if (fs.exists(new Path(outputPath))){
            fs.delete(new Path(outputPath),true);
        }

        //获取job实例
        Job job = Job.getInstance(conf, "mapJoin");

        //设置主类
        job.setJarByClass(EmpJobDriver.class);

        //设置mapper 和输出格式
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Employee.class);

        //设置reducer 和输出格式
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Employee.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
