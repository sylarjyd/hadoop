package cn.jyd.service;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class TestFileSystemURL {

	static {

		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

	}


	/**

	 * 通过URL对象读取HDFS文件数据

	 * 

	 * @author Administrator

	 * 

	 * 想要从HDFS中读取数据，首先我们考虑的是从WEBUI的那种方式去取出数据

	 * 1.定义一个URL对象，该URL对象封装了该HDFS的URL地址信息

	 * 2.调用openStream()方法打开一个输入流

	 * 3.通过IOUtils工具类把输入流复制到字节数组输出流中，然后打印输出字节输出流的信息

	 * 

	 */

	@org.junit.Test

	public void readByURL() throws Exception {

		// 配置需要读取的URL地址的文件

		String urlStr = "hdfs://localhost:9000/test/test01.txt";
//		String urlStr = "hdfs://localhost:9000/test.txt";

		// 通过URL的带string的构造方法来创建URL对象

		URL url = new URL(urlStr);

		// 调用URL对象的openStream()方法得到一个inputstream对象

		InputStream is = url.openStream();

		//定义一个字符数组

//		byte[] bys = new byte[1024];

//		int len =0;

//有三部，读取，赋值，判断，一次读取一个字符数组，返回值是读取的字符数组的长度，如果已经到

//文件的末尾了，就返回-1

//		while((len=is.read(bys))!=-1){

//输出一个字符数组，准确的说是输出字符数组的一部分，从0开始到读取的长度结束，print后面不加

//ln

//			System.out.print(new String(bys,0,len));

//		}

		// 创建字节数组输出流对象

		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		// hadoop提供的IOUtils方法来实现流的对拷

		IOUtils.copyBytes(is, baos, 1024);
		
		// 关闭流对象

		IOUtils.closeStream(is);

		System.out.println(new String(baos.toByteArray()));

   }

}

