package cn.hadoop.mapreduce.recordreader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/4/5
 */
public class MyInputFormat extends FileInputFormat<LongWritable, Text> {


    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //返回自定义的RecodrdReader
        return new RecordReaderApp.MyRecordReader();
    }

    /**
     * 为了使切分数据的时候行号不发生错乱，这里设置不进行切分
     * @param fs
     * @param filename
     * @return
     */
    public boolean isSplitable(FileSystem fs, Path filename){
        return false;
    }


}
