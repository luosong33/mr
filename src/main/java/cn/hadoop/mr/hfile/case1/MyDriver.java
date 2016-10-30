package cn.hadoop.mr.hfile.case1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MyDriver {
	
	private static Configuration conf = null;
    static {
    	conf = HBaseConfiguration.create();// 配置
    	conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
    }
    
    /**
     * @param args  hdfs://node1:8020/ls/testdata/input1 hdfs://node1:8020/ls/testdata/output
     * vm    -DHADOOP_USER_NAME=root
     * @throws Exception
     */
	public static void main(String[] args) throws Exception {
		
		Job job = new Job(conf, "toHFile");
		job.setJarByClass(MyDriver.class);

		job.setMapperClass(MyMapper.class); // 指定job使用的mapper/reducer类
		job.setReducerClass(KeyValueSortReducer.class);
//		job.setReducerClass(MyReduce.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class); // 指定mapper输出数据的kv类型
		job.setMapOutputValueClass(KeyValue.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class); // 指定最终输出数据的kv类型
		job.setOutputValueClass(KeyValue.class);
		job.setInputFormatClass(TextInputFormat.class); // ?
		job.setOutputFormatClass(HFileOutputFormat.class);
		/*HTable table = new HTable(conf,"test_car_prpcmain".getBytes());
		HFileOutputFormat.configureIncrementalLoad(job,table);*/
		FileInputFormat.addInputPath(job, new Path(args[0])); // 指定job的数据源、结果目录
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void createTab(String tabName) throws Exception {
		Configuration conf = new Configuration();
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tabName)) {
		System.out.println(tabName + " exists!");
		admin.close();
		return;
		}
		HTableDescriptor table = new HTableDescriptor(tabName);
		table.addFamily(new HColumnDescriptor("f1"));
		table.addFamily(new HColumnDescriptor("f2"));
		table.addFamily(new HColumnDescriptor("f3"));
		table.getFamily(Bytes.toBytes("f1"));
		admin.createTable(table);
		admin.close();
	}
}
// job.setMapperClass(MyMapper.class);