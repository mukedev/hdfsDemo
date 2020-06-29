package cn.bxg.exam;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangYu
 * @date 2020/6/29
 */
public class JoinReduce extends Reducer<Text, Text, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> list = new ArrayList();
        List<String> frists = new ArrayList();

        for (Text text : values) {
            String s = text.toString().split(",")[0];
            if (s.indexOf(".") > 0) {
                list.add(text.toString());
            } else {
                frists.add(text.toString());
            }
        }
        if (frists == null || frists.size() == 0) {
            System.exit(1);
        }

        for (String frist : frists) {
            String startTime = frist.split(",")[2];
            String endTime = frist.split(",")[4];

            for (String str : list) {
                String[] line = str.split(",");
                String visitTime = line[1];
                startTime.compareTo(visitTime);
                if (visitTime.compareTo(startTime) > 0 && endTime.compareTo(visitTime) > 0) {
                    context.write(new Text(frist + "," + str), NullWritable.get());
                }
            }
        }
    }
}

