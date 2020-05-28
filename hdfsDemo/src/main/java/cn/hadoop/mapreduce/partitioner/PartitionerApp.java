package cn.hadoop.mapreduce.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author zhangYu
 * @date 2020/4/5
 */
public class PartitionerApp {

    private static class MyMapper extends Mapper<LongWritable, Text,Text, IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("#########err");
            System.out.println(value.toString());
            String[] s = value.toString().split("\t");
            context.write(new Text(s[0]),new IntWritable(Integer.parseInt(s[1])));
        }
    }

    private static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : value) {
                sum += val.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }


    public static class MyPartitioner extends Partitioner<Text,IntWritable>{

        //转发给4个不同的reducer
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions){
            if (key.toString().equals("xiaomi"))
                return 0;
            if (key.toString().equals("huawei"))
                return 1;
            if (key.toString().equals("iphone7"))
                return 2;
            return 3;
        }
    }

    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
        String inPath = "hdfs://192.168.144.128:9000/partitioner";
        String outPath = "hdfs://192.168.144.128:9000/outputpartitioner";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(inPath), conf);
        if (fs.exists(new Path(outPath))){
            fs.delete(new Path(outPath),true);
        }

        Job job = Job.getInstance(conf, "PartitionerApp");

        //运行jar类
        job.setJarByClass(PartitionerApp.class);

        //设置map
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置partitioner
        job.setPartitionerClass(MyPartitioner.class);
        //设置4个reducer 每个分区一个
        job.setNumReduceTasks(4);

        //输入格式设置
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job,new Path(inPath));

        //输出格式设置
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,new Path(outPath));

        //提交job
        System.exit(job.waitForCompletion(true)?0:1);
    }


}
