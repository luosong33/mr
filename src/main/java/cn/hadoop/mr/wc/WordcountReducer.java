package cn.hadoop.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordcountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	/**
	 * reduceTask收到很多map的key，value，mr框架会将key进行分组汇合
	 * 一组调一次，根据传的第一个key和迭代器Iterable得到汇合
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		
		for(IntWritable value : values){
			count += value.get();
		}
		context.write(key, new IntWritable(count)); // 具体输出到哪里有框架决定
		
	}

}
