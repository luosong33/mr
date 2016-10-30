package cn.hadoop.mr.wc;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * keyin 默认为mr框架所读取的一行文本的起始偏移量，Long，但是mr有更好的序列化接口，所有用LongWritable
 * valuein 默认为mr框架所读取的一行文本的内容，同上，用Text
 * keyout 用户自定义逻辑处理完成后输出数据的key，此处为单词，String
 * valueout 用户自定义逻辑处理完成后输出数据的内容，此处是次数，IntWritable
 * @author Administrator
 *
 */

public class WordcountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

	/**
	 * maptask会对每一行输入的数据调用一次map，一行调一次map方法
	 * 编写业务逻辑：切分->输出k，v格式
	 */
	@Override
	protected void map(LongWritable key, Text value,Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = line.split(" ");
		
		for (String word : words) {
			context.write(new Text(word),new IntWritable(1));
		}
	}

	
}
