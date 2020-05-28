package cn.hadoop.mapreduce.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/4/6
 */
public class MyMapper extends Mapper<LongWritable, Text,LongWritable,Employee> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String val = value.toString();
        String arr[] = val.split("\t");

        System.out.println("arr.length=" + arr.length + " arr[0]=" + arr[0]);

        if (arr.length <= 3) {
            Employee dept = new Employee();
            dept.setDeptNo(arr[0]);
            dept.setDeptName(arr[1]);
            dept.setFlag(1);
            context.write(new LongWritable(Long.valueOf(dept.getDeptNo())),dept);
        } else {
            Employee e = new Employee();
            e.setEmpNo(arr[0]);
            e.setEmpName(arr[1]);
            e.setDeptNo(arr[4]);
            e.setFlag(0);
            context.write(new LongWritable(Long.valueOf(e.getDeptNo())),e);
        }
    }
}
