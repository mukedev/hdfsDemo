package cn.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

/**
 * @author zhangYu
 * @date 2020/4/5
 */
public class WordCountCombinerApp {

    public static class TokenizerMapper extends Mapper<Object, Text,Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word,one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key,result);
        }
    }


    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {

        String inPath = "hdfs://192.168.144.128:9000/wc";
        String outPath = "hdfs://192.168.144.128:9000/outputwc";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(inPath), conf);
        if (fs.exists(new Path(outPath))){
            fs.delete(new Path(outPath),true);
        }

        Job job = Job.getInstance(conf, "word count");

        //运行jar类
        job.setJarByClass(WordCountCombinerApp.class);


        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入格式
        Path inputPath = new Path(inPath);
        FileInputFormat.addInputPath(job,inputPath);

        //设置输出格式
        Path outputPath = new Path(outPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        System.exit(job.waitForCompletion(true)?0:1);

    }

}
