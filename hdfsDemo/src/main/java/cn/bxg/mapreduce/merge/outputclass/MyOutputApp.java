package cn.bxg.mapreduce.merge.outputclass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author zhangYu
 * @date 2020/6/14
 */
public class MyOutputApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        String inPath = "hdfs://had-node:8020/mergeFile/outputclass/input";
        String outPath = "hdfs://had-node:8020/mergeFile/outputclass/output";

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(inPath), conf);
        if (fileSystem.exists(new Path(outPath))) {
            fileSystem.delete(new Path(outPath), true);
        }

        //获取job实例并设置主类
        Job job = Job.getInstance(conf, "outputFormat_text");
        job.setJarByClass(MyOutputApp.class);

        /**
         * mapreduce八大步骤
         */
        //设置文件的输入路径和输入类
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(inPath));
        //设置mapper类和kv数据类型
        job.setMapperClass(MyOutputMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //排序
        //排序
        //规约
        //分组
        //设置reduce类和kv数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输出类和输出路径
        job.setOutputFormatClass(MyOutputFormat.class);
        MyOutputFormat.setOutputPath(job, new Path(outPath));

        //推出
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
