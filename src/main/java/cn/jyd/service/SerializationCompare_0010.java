package cn.jyd.service;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

    //测试使用Hadoop序列化和JDK序列化之间的区别
public class SerializationCompare_0010{
    //Writable是Hadoop中所有数据类型的父类（父接口）。
    public static byte[] serialize(Writable writable) throws IOException{
        //这是一种编程思想，因为我们返回的是一个字节数组，所以进行了一下流的转换。
        ByteArrayOutputStream baos=
            new ByteArrayOutputStream();
        ObjectOutputStream oos=
            new ObjectOutputStream(baos);
        writable.write(oos);
        oos.close();
        return baos.toByteArray();
    }

    //能序列化的一定是类类型，所以这里使用int类型的包装类
    public static byte[] serialize(Integer integer) throws IOException{
        ByteArrayOutputStream baos=
            new ByteArrayOutputStream();
        ObjectOutputStream oos=
            new ObjectOutputStream(baos);
        oos.writeInt(integer);
        oos.close();
        return baos.toByteArray();
    }

    public static Writable deserialize(byte[] bytes) throws IOException{
        ByteArrayInputStream bais=
            new ByteArrayInputStream(bytes);
        DataInputStream dis=
            new DataInputStream(bais);
        IntWritable iw=new IntWritable();
        iw.readFields(dis);
        return iw;
    }

    public static void main(String[] args) throws IOException{
        IntWritable iw=new IntWritable(200);
        //hadoop也可以使用set方法传值
        // iw.set(300);
        byte[] bytes=serialize(iw);
        System.out.println("Hadoop："+bytes.length);
        //Writable deIw=deserialize(bytes);
        //System.out.println("Hadoop Deserialize："+deIw);

        Integer integer=new Integer(200);
        bytes=serialize(integer);
        System.out.println("Java："+bytes.length);
    }
}