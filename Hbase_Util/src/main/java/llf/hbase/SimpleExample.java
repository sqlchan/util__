package llf.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.net.URISyntaxException;

public class SimpleExample {
    public static void main(String[] args) throws URISyntaxException, IOException {
        Configuration config=HBaseConfiguration.create();
        config.addResource(new Path(ClassLoader.getSystemResource("hdfs-site.xml").toURI()));
        config.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));

        try (Connection connection=ConnectionFactory.createConnection(config); Admin admin=connection.getAdmin()){
            TableName tableName = TableName.valueOf("mytable");
            HTableDescriptor table = new HTableDescriptor(tableName);
            HColumnDescriptor mycf = new HColumnDescriptor("mycf");
            table.addFamily(new HColumnDescriptor(mycf));

            admin.createTable(table);
        }
    }
}
