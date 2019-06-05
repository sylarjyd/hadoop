package cn.jyd.service;
import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
  
 public class CountNum {
     /**
      * 四个泛型类型分别代表：
      * KeyIn        Mapper的输入数据的Key，这里是每行文字的起始位置（0,11,...）
      * ValueIn      Mapper的输入数据的Value，这里是每行文字
      * KeyOut       Mapper的输出数据的Key，这里是每行文字中的“年份”
      * ValueOut     Mapper的输出数据的Value，这里是每行文字中的“气温”
      */
     static class TempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
         @Override
         public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
             // 打印样本: Before Mapper: 0, 98680961121
//             System.out.print("Before Mapper: " + key + ", " + value);
             String line = value.toString();
             char[] charArray = line.toCharArray();
             int count = 1;
//             System.out.println(
//                     "======" +
//                     "After Mapper:");
             for (int i = 0; i < charArray.length; i++) {
            	 context.write(new Text(charArray[i]+""), new IntWritable(count));
            	 // 打印样本: After Mapper:9, 1
//                 System.out.println(new Text(charArray[i]+"") + ", " +  new IntWritable(count));
			}
            
         }
     }
  
     /**
      * 四个泛型类型分别代表：
      * KeyIn        Reducer的输入数据的Key，这里是每行文字中的“年份”
      * ValueIn      Reducer的输入数据的Value，这里是每行文字中的“气温”
      * KeyOut       Reducer的输出数据的Key，这里是不重复的“年份”
      * ValueOut     Reducer的输出数据的Value，这里是这一年中的“最高气温”
      */
     static class TempReducer extends
             Reducer<Text, IntWritable, Text, IntWritable> {
         @Override
         public void reduce(Text key, Iterable<IntWritable> values,
                 Context context) throws IOException, InterruptedException {
             int count = 0;
//             StringBuffer sb = new StringBuffer();
             for (IntWritable value : values) {
            	 count += value.get();
//            	 sb.append(value.get()+",");
             }
             // 打印样本： Before Reduce: 9,1,1,1,1 
//             System.out.print("Before Reduce: " + key + ", " + sb.toString());
             context.write(key, new IntWritable(count));
             // 打印样本： After Reduce: 9, 99
             System.out.println("======" +"After Reduce: " + key + ", " + count);
         }
     }
  
     public static class ValueComparator extends WritableComparator {
         protected ValueComparator() {
             super(IntWritable.class, true);
         }
  
         @Override
         public int compare(WritableComparable w1, WritableComparable w2) {
             int v1 = Integer.parseInt(w1.toString());
             int v2 = Integer.parseInt(w2.toString());  
             return v1 - v2;
         }
     }
     
     public static void main(String[] args) throws Exception {
         //输入路径
         String dst = "hdfs://localhost:9000/shuju.txt";
         //输出路径，必须是不存在的，空文件加也不行。
         String dstOut = "hdfs://localhost:9000/shujucount";
         Configuration hadoopConfig = new Configuration();
          
         hadoopConfig.set("fs.hdfs.impl", 
             org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
         );
         hadoopConfig.set("fs.file.impl",
             org.apache.hadoop.fs.LocalFileSystem.class.getName()
         );
         
         Job job = new Job(hadoopConfig);
          
         //如果需要打成jar运行，需要下面这句
         //job.setJarByClass(NewMaxTemperature.class);
  
         //job执行作业时输入和输出文件的路径
         FileInputFormat.addInputPath(job, new Path(dst));
         FileOutputFormat.setOutputPath(job, new Path(dstOut));
  
         //指定自定义的Mapper和Reducer作为两个阶段的任务处理类
         job.setMapperClass(TempMapper.class);
         job.setReducerClass(TempReducer.class);
         
         //设置最后输出结果的Key和Value的类型
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(IntWritable.class);
          
         //执行job，直到完成
         job.waitForCompletion(true);
         System.out.println("Finished");
     }
 }