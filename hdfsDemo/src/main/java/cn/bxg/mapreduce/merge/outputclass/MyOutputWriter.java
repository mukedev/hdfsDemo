package cn.bxg.mapreduce.merge.outputclass;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author zhangYu
 * @date 2020/6/14
 */
public class MyOutputWriter extends RecordWriter<Text, NullWritable> {

    private OutputStream goodCommentOutputStream;
    private OutputStream badCommentOutputStream;

    public MyOutputWriter(OutputStream goodCommentOutputStream, OutputStream badCommentOutputStream) {
        this.goodCommentOutputStream = goodCommentOutputStream;
        this.badCommentOutputStream = badCommentOutputStream;
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        //1.从文本数据获取第九个字段
        String[] split = text.toString().split("\t");
        String numStr = split[9];

        //2.根据字段的值，判断评论的类型，然后将对应的数据写入不同的文件夹中
        if (Integer.parseInt(numStr) <= 1) {
            //好评或者中评
            goodCommentOutputStream.write(text.toString().getBytes());
            goodCommentOutputStream.write("\r\n".getBytes());
        } else {
            //差评
            badCommentOutputStream.write(text.toString().getBytes());
            badCommentOutputStream.write("\r\n".getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(goodCommentOutputStream);
        IOUtils.closeStream(badCommentOutputStream);
    }
}
