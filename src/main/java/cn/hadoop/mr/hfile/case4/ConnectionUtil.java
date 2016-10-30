package cn.hadoop.mr.hfile.case4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Administrator on 2016/10/21.
 */
public class ConnectionUtil {

    /**
     * 配置
     */
    private static Configuration config = null;
    static {
        config = HBaseConfiguration.create();// 配置
        /*config.set("fs.defaultFS", "hdfs://mycluster");*/
        config.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
    }

    Connection conn = null;
    {
        Configuration config = HBaseConfiguration.create();// 配置
        config.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        try {
            conn = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static Set<String> tables = new HashSet<>();
    public void createTable(String tableName) throws IOException {
        synchronized (tables) {
            if (!tables.add(tableName)) return;
        }
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            TableName table = TableName.valueOf(tableName);
            if (!admin.tableExists(table)) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("info");
                hTableDescriptor.addFamily(hColumnDescriptor);
                admin.createTable(hTableDescriptor);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            admin.close();
        }
    }

    public static Configuration getConfiguration() {
        return config;
    }

    public static HTable getTable() throws IOException {
        HTable table = new HTable(config, "test_car_prpcmain");
        return table;
    }

    public static void main(String[] args) throws IOException {
        new ConnectionUtil().createTable("test_car_prpcmain"); // 可本地运行，与本地hadoop环境无关
    }
}
