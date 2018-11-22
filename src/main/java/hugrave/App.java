package hugrave;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.io.file.tfile.ByteArray;
import sun.jvm.hotspot.runtime.Bytes;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        try {
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "192.168.0.102");
            config.set("hbase.zookeeper.property.clientport", "2181");

            HBaseAdmin.checkHBaseAvailable(config);
            System.out.println("Connection successful!");

            Connection conn = ConnectionFactory.createConnection(config);
            try {

//                HBaseAdmin admin = new HBaseAdmin(config);
//                admin.disableTable("test");
//                admin.deleteTable("test");
//                System.out.println("delete table  ok.");

                Table test = conn.getTable(TableName.valueOf("test"));

                Put p = new Put("row2".getBytes());
                p.add("cf".getBytes(), "cq".getBytes(), "Hello new World!".getBytes());
                test.put(p);

                Get g = new Get("row2".getBytes());
                Result r = test.get(g);
                byte[] value = r.getValue("cf".getBytes(), "cq".getBytes());
                String resultString = new String(value);
                System.out.println(resultString);
            } finally {
                conn.close();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }
}
