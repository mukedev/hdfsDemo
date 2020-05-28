package cn.hadoop.mapreduce.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author zhangYu
 * @date 2020/4/6
 */
public class MyReducer extends Reducer<LongWritable,Employee, NullWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Employee> values, Context context) throws IOException, InterruptedException {
        Employee dept = null;
        ArrayList<Employee> list = new ArrayList<>();

        for (Employee emp:values) {
            if (emp.getFlag() == 0){
                Employee employee = new Employee(emp);
                list.add(employee);
            } else {
                dept = new Employee(emp);
            }
        }

        if (dept != null) {
            for (Employee emp : list) {
                emp.setDeptName(dept.getDeptName());
                context.write(NullWritable.get(),new Text(emp.toString()));
            }
        }

    }
}
