package cn.hadoop.mr.hfile.case1;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReduce extends Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, KeyValue> {     

	protected void reduce(ImmutableBytesWritable key, Iterable<Text> value,Context context)
			throws IOException, InterruptedException {
		String[] values = value.toString().split(","); // 切分每行数据
		byte[] rowKey = Bytes.toBytes(values[0]);
		
		ImmutableBytesWritable rKey = new ImmutableBytesWritable(rowKey);
		KeyValue kv = new KeyValue(rowKey, Bytes.toBytes(values[1]),
				Bytes.toBytes(values[2]), Bytes.toBytes(values[3]));
		context.write(rKey, kv);
		
	}
	
}
