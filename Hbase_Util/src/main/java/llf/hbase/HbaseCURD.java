package llf.hbase;

import com.sun.xml.internal.bind.v2.schemagen.xmlschema.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;

public class HbaseCURD {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration config=HBaseConfiguration.create();
        config.addResource(new Path(ClassLoader.getSystemResource("hdfs-site.xml").toURI()));
        config.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));
        try (Connection connection=ConnectionFactory.createConnection(config)){
            Table table = connection.getTable(TableName.valueOf("mytable"));

            Put put = new Put(Bytes.toBytes("row1"));
            put.addColumn(Bytes.toBytes("mycf"),Bytes.toBytes("name"),Bytes.toBytes("ted"))
                    .addColumn(Bytes.toBytes("mycf"),Bytes.toBytes("name"),Bytes.toBytes("ted"));
            table.put(put);

            Put put1 = new Put(Bytes.toBytes("row1"));
            put1.addColumn(Bytes.toBytes("mycf"),Bytes.toBytes("name"),Bytes.toBytes("ted"));
            boolean result = table.checkAndPut(Bytes.toBytes("row1"),Bytes.toBytes("mycf"),Bytes.toBytes("name"),
                    Bytes.toBytes("jack"),put1);

            boolean result1 = table.checkAndPut(Bytes.toBytes("row1"),Bytes.toBytes("mycf"),Bytes.toBytes("name"),
                    null,put1);

            boolean result2 = table.checkAndPut(Bytes.toBytes("row1"),Bytes.toBytes("mycf"),Bytes.toBytes("name"),
                    CompareFilter.CompareOp.LESS,Bytes.toBytes("1"),put1);

            Append append= new Append(Bytes.toBytes("row1"));
            append.add(Bytes.toBytes("mycf"),Bytes.toBytes("name"),Bytes.toBytes("jack"));
            table.append(append);

            Increment inc = new Increment(Bytes.toBytes("row1"));
            inc.addColumn(Bytes.toBytes("mycf"),Bytes.toBytes("name"),10L);
            table.increment(inc);

            Get get= new Get(Bytes.toBytes("row1"));
            Result result3 = table.get(get);
            //byte[] name = result3.getValue(Bytes.toBytes("mycf"),Bytes.toBytes("name"));
            java.util.List <Cell> cells = result3.getColumnCells(Bytes.toBytes("mycf"),Bytes.toBytes("name"));
            for(Cell c : cells){
                byte[] cValue = CellUtil.cloneValue(c);
            }

            boolean isexit = table.exists(get);

            Delete delete = new Delete(Bytes.toBytes("row1"));
            delete.addColumn(Bytes.toBytes("mycf"),Bytes.toBytes("name"));
            boolean isdelete = table.checkAndDelete(Bytes.toBytes("row1"),Bytes.toBytes("mycf"),Bytes.toBytes("name"),Bytes.toBytes("haah"), delete);

            //mutation
            RowMutations rowMutations = new RowMutations(Bytes.toBytes("row1"));
            rowMutations.add(delete);
            rowMutations.add(put);
            table.mutateRow(rowMutations);

            java.util.List<Row> actions =new ArrayList<>();
            actions.add(get);
            actions.add(put);
            actions.add(delete);
            Object[] results = new Object[actions.size()];
            table.batch(actions,results);

            Scan scan = new Scan(Bytes.toBytes("row1"));
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs){
                String name = Bytes.toString(r.getValue(Bytes.toBytes("mycf"),Bytes.toBytes("name")));
            }
            rs.close();



        }
    }
}
