package cn.hadoop.mr.hfile.case4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

//import zl.hbase.util.ConnectionUtil;

public class HFileGenerator_ {

    /**
     * 读取处理数据
     */
    public static class HFileMapper extends
            Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            //String[] items = line.split(",", -1); // 参考http://blog.csdn.net/wguangliang/article/details/52526891
            String[] items = line.split("\\u003E\\u003E\\u007C", -1);
            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(
                    items[0].getBytes());

            KeyValue kv = new KeyValue(Bytes.toBytes(items[0]),
                    Bytes.toBytes(items[1]), Bytes.toBytes(items[2]),
                    System.currentTimeMillis(), Bytes.toBytes(items[3])
                    /*Bytes.toBytes(items[4]), Bytes.toBytes(items[5]),
                    Bytes.toBytes(items[6]), Bytes.toBytes(items[7])*/);
            if (null != kv) {
                context.write(rowkey, kv);
            }
        }
    }


    /**
     *
     * @param args  hdfs://node1:8020/ls/testdata/input hdfs://node1:8020/ls/testdata/output    output1不能存在（hadoop的机制）
     * vm  -DHADOOP_USER_NAME=root
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        /*Configuration conf = new Configuration();*/

        Configuration conf = ConnectionUtil.getConfiguration();
        String[] dfsArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();

        Job job = new Job(conf, "HFile bulk load test");
        job.setJarByClass(HFileGenerator_.class);

        job.setMapperClass(HFileMapper.class);
        job.setReducerClass(KeyValueSortReducer.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(SimpleTotalOrderPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(dfsArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(dfsArgs[1]));

        HFileOutputFormat.configureIncrementalLoad(job, ConnectionUtil.getTable());
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}