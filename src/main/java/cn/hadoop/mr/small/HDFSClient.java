package cn.hadoop.mr.small;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HDFSClient {

	private static Configuration config = null;

	// private static Configuration config=new Configuration();
	static {
		config = HBaseConfiguration.create(); // 配置
		// config.set("fs.defaultFS", "hdfs://mycluster");
		config.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181"); // 在hosts文件配置好
	}

	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {

		/*
		 * config.set("fs.defaultFS", "hdfs://10.133.250.70:9000");
		 * config.set("dfs.replication", "3");
		 */

		Iterator<Entry<String, String>> it = config.iterator();
		while (it.hasNext()) {
			Entry<String, String> ent = it.next();
			System.out.println(ent.getKey() + " : " + ent.getValue());
		}
		FileSystem fs = FileSystem.get(config);
		// FileSystem fs = FileSystem.get(new URI("hdfs://10.133.250.70:9000"),
		// config, "root");

		Path dst = new Path("/ls/lsTestasd.txt");
		Path src = new Path("c:/test.txt"); // c:\\test.txt也可以
		// Thread.sleep(500000);

		// FSDataOutputStream outputStream=fs.create(dst);
		// fs.create(dst);//创建文件
		// fs.mkdirs(dst);//新建文件夹
		fs.copyFromLocalFile(src, dst);
		System.out.println("Upload to" + config.get("fs.default.name"));

		FileStatus files[] = fs.listStatus(dst);
		for (FileStatus file : files) {
			System.out.println(file.getPath());
		}
		fs.close();
	}
}
