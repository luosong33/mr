package cn.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WorkcountDriver {
	
	private static Configuration conf = null;
	static {
		conf = HBaseConfiguration.create(); // 配置
		conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181"); // 在hosts文件配置好
	}
	
	/**
	 * 驱动程序
	 * 封装参数和jar，提交给yarn
	 * eclipse中运行参数：args hdfs://node1:8020/ls/wc/input hdfs://node1:8020/ls/wc/output1    output1不能存在（hadoop的机制）
	 * vm  -DHADOOP_USER_NAME=root
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
//		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		// 指定job使用的mapper/reducer类
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		
		// 指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 指定最终输出数据的kv类型(reducer的输出格式,有时候业务不需要汇总，就不用定义这个，比如改变大小写)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 指定job的数据源目录
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		// 指定job的结果目录
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		// 指定本程序jar包所在的路径
		/*job.setJar("/home/hadoop/wr.jar"); // 写死之后不够灵活，不推荐*/
		job.setJarByClass(WorkcountDriver.class);
		
		// 提交之后，把jar包拷给yarn集群启动，进行分布式运行,最终生成一个job.xml文件
		/*job.submit(); // 但是不知道运行后的情况，不推荐*/		
		System.exit(job.waitForCompletion(true) ? 0 : 1); // true 返回执行信息
		
		// 但是他是如何知道是要往yarn提交，还有yarn的地址的
		// win下可以先设置，再修改源码去拦截yarn提交前的参数封装
		// win下不兼容是因为封装的参数有%和$
		// linux下new  Configuration会自动读取到配置，只需打成jar包
		// 导成普通jar文件，用的引用jar依赖，依然可以运行，是因为运行的时候，会将hadoop的安装目录全部export到classpath下
		
	}

}
