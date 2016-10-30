package cn.hadoop.mr.hfile.case1;

import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 生成HFile文件
 * @author Administrator
 *
 */
public class MyMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	//private ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable();
	
	protected void map(LongWritable key, Text value, Context context)
	//protected void map(LongWritable key, KeyValue value, Context context)
			throws IOException, InterruptedException {
		String[] values = value.toString().split(","); // 切分每行数据
		byte[] rowKey = Bytes.toBytes(values[0]);
		
		ImmutableBytesWritable rKey = new ImmutableBytesWritable(rowKey);
		KeyValue kv = new KeyValue(rowKey, Bytes.toBytes(values[1]),
				Bytes.toBytes(values[2]),System.currentTimeMillis(), Bytes.toBytes(values[3]));
		context.write(rKey, kv);
		/*immutableBytesWritable.set(Bytes.toBytes(key.get())); 
        context.write(immutableBytesWritable, value); */
	}
}