package llf.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;
import java.net.URISyntaxException;

public class HbaseTable {
    public static void createOrOverWrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())){
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public static void createSchemaTables(Configuration conf) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf); Admin admin=connection.getAdmin()){
            HTableDescriptor table = new HTableDescriptor((TableName.valueOf("mytable")));
            table.addFamily(new HColumnDescriptor("mycf").setCompressionType(Compression.Algorithm.NONE));
            System.out.println("create table");
            createOrOverWrite(admin, table);
            System.out.println("done.");
        }
    }

    public static void modifySchema (Configuration conf) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf); Admin admin=connection.getAdmin()){
            TableName tableName = TableName.valueOf("mytable");
            if(!admin.tableExists(tableName)){
                System.out.println("table not exit");
                System.exit(-1);
            }
            HColumnDescriptor newcolumn = new HColumnDescriptor("newcf");
            newcolumn.setCompactionCompressionType(Compression.Algorithm.GZ);
            newcolumn.setMaxVersions(HConstants.ALL_VERSIONS);
            admin.addColumn(tableName,newcolumn);

            HTableDescriptor table = admin.getTableDescriptor(tableName);
            HColumnDescriptor mycf = new HColumnDescriptor("mycf");
            mycf.setCompactionCompressionType(Compression.Algorithm.GZ);
            mycf.setMaxVersions(HConstants.ALL_VERSIONS);
            table.modifyFamily(mycf);
            admin.modifyTable(tableName,table);
        }
    }

    public static void deleteSchema(Configuration conf) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf); Admin admin=connection.getAdmin()){
            TableName tableName = TableName.valueOf("mytable");
            admin.disableTable(tableName);
            admin.deleteColumn(tableName,"mycf".getBytes("UTF-8"));
            admin.deleteTable(tableName);
        }
    }

    public static void main(String[] args) throws URISyntaxException, IOException {
        Configuration config = HBaseConfiguration.create();
        config.addResource(new Path(ClassLoader.getSystemResource("hdfs-site.xml").toURI()));
        config.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));

        createSchemaTables(config);
        modifySchema(config);
        deleteSchema(config);
    }
}
