package cn.hadoop.mr.hfile.case1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
//import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;

/**
 * HFile文件载入HBase
 * @author Administrator
 *
 */
public class LoadData {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("hbase.metrics.showTableName", "false");
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTable table = new HTable(conf, "xtab");
		//SchemaMetrics.configureGlobally(conf);
		loader.doBulkLoad(new Path(args[0]), table);
		table.flushCommits();
		table.close();
		admin.close();
	}
}