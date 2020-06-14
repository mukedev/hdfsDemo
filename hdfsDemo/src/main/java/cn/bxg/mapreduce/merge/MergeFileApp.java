package cn.bxg.mapreduce.merge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author zhangYu
 * @date 2020/6/13
 */
public class MergeFileApp {

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        String inPath = "hdfs://had-node:8020/mergeFile/input";
        String outPath = "hdfs://had-node:8020/mergeFile/output";

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(inPath), conf);
        if (fileSystem.exists(new Path(outPath))) {
            fileSystem.delete(new Path(outPath), true);
        }

        //获取job实例并设置主类
        Job job = Job.getInstance(conf, "mergeFile_test");
        job.setJarByClass(MergeFileApp.class);

        /**
         * 八大步骤
         */
        //1.设置输入路径和读取方式
        job.setInputFormatClass(MyInputFormat.class);
        MyInputFormat.addInputPath(job, new Path(inPath));
        //2.设置mapper主类和设置kv数据类型
        job.setMapperClass(MergeFileMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        //3.分区
        //4.排序
        //5.规约
        //6.分组
        //7.不需要设置Reducer类,但是必须设置数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        //8.设置输出路径和写入方式
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(outPath));

        //退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
