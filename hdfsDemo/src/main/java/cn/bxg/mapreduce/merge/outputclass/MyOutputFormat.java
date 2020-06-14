package cn.bxg.mapreduce.merge.outputclass;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/6/14
 */
public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //1.获取目标文件的输出流
        Path path = new Path("hdfs://had-node:8020");
        FileSystem fileSystem = path.getFileSystem(taskAttemptContext.getConfiguration());
        FSDataOutputStream goodCommentOutputStream = fileSystem.create(new Path("hdfs://had-node:8020/mergeFile/outputclass/outputData1"));
        FSDataOutputStream badCommentOutputStream = fileSystem.create(new Path("hdfs://had-node:8020/mergeFile/outputclass/outputData2"));


        //2.将输出流传给MyOutputWriter
        MyOutputWriter myOutputWriter = new MyOutputWriter(goodCommentOutputStream, badCommentOutputStream);

        return myOutputWriter;
    }
}
