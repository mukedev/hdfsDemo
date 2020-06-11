package cn.bxg.mapreduce.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhangYu
 * @program hdfsDemo
 * @description
 * @create 2020-06-11 11:31
 **/
public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String first = "";
        String last = "";
        for (Text val : values) {
            String[] line = val.toString().split(",");
            if (line[0].startsWith("p")) {
                first = val.toString();
            } else {
                last += "," + val.toString();
            }
        }

        context.write(key, new Text(first + last));
    }
}
