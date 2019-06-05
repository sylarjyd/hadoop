package cn.jyd.service;

import java.io.FileInputStream;
import java.io.FileOutputStream;  
import java.io.IOException;  
import java.io.InputStream;  
import java.io.OutputStream;  
import java.net.URI;  
import java.net.URISyntaxException;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IOUtils;

public class Test {
	public static void main(String[] args) throws IOException, URISyntaxException {
		// 获得FileSystem对象  
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());  
        // 调用open方法进行下载，参数HDFS路径  
        InputStream in = fileSystem.open(new Path("/hadoop/sqlfile20190523.sql")); 

        // 创建输出流，参数指定文件输出地址  
        OutputStream out = new FileOutputStream("D:\\sylar\\tmp\\database\\sqlfile20190523-2.sql");  
        // 使用Hadoop提供的IOUtils，将in的内容copy到out，设置buffSize大小，是否关闭流设置true  
        IOUtils.copyBytes(in, out, 409600, true);  
	}
	
	@org.junit.Test
	public void method_1() throws IOException, InterruptedException, URISyntaxException {
		 // 获得FileSystem对象，指定使用root用户上传  
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());  
        // 创建输入流，参数指定文件输出地址  
        InputStream in = new FileInputStream("D:\\sylar\\tmp\\database\\sqlfile20190523.sql");  
        // 调用create方法指定文件上传，参数HDFS上传路径  
        OutputStream out = fileSystem.create(new Path("/hadoop/sqlfile20190523.sql"));  
        // 使用Hadoop提供的IOUtils，将in的内容copy到out，设置buffSize大小，是否关闭流设置true  
        IOUtils.copyBytes(in, out, 4096, true); 
	}
}
