package cn.bxg.mapreduce.friend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/6/13
 */
public class MFReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for (Text val : values) {
            sb.append(val + "-");
        }
        context.write(key, new Text(sb.toString()));
    }
}
