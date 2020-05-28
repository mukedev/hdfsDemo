package cn.hd;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 序列化实体类
 * @author zhangYu
 * @date 2020/4/4
 */
public class Person implements WritableComparable<Person> {

    private Text name = new Text();
    private IntWritable age = new IntWritable();
    private Text sex = new Text();

    public Person(String name, int age, String sex) {
        this.name.set(name);
        this.age.set(age);
        this.sex.set(sex);
    }

    public Person(Text name, IntWritable age, Text sex) {
        this.name = name;
        this.age = age;
        this.sex = sex;
    }

    public Person(){

    }

    public void set(String name, int age, String sex){
        this.name.set(name);
        this.age.set(age);
        this.sex.set(sex);
    }

    @Override
    public int compareTo(Person o) {

        int comp1 = name.compareTo(o.name);
        if (comp1 != 0) {
            return comp1;
        }

        int comp2 = age.compareTo(o.age);
        if (comp2 != 0) {
            return comp2;
        }

        int comp3 = sex.compareTo(o.sex);
        if (comp3 != 0) {
            return comp3;
        }

        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        name.write(dataOutput);
        age.write(dataOutput);
        sex.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        name.readFields(dataInput);
        age.readFields(dataInput);
        sex.readFields(dataInput);
    }

    @Override
    public int hashCode(){
        final int prime = 31;
        int result = 1;
        result = prime * result + ((age == null)?0:age.hashCode());
        result = prime * result + ((name == null)?0:name.hashCode());
        result = prime * result + ((sex == null)?0:sex.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        return Objects.equals(name, person.name) &&
                Objects.equals(age, person.age) &&
                Objects.equals(sex, person.sex);
    }

    @Override
    public String toString() {
        return "Person{" +
                "name=" + name +
                ", age=" + age +
                ", sex=" + sex +
                '}';
    }
}
