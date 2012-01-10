package poseidon.tests;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.util.Hashtable;

/**
 * @author Ashwin
 */
public class TestUtil {

    private static final String port = "21818";

    private static final HBaseTestingUtility testUtil = new HBaseTestingUtility();


    public static void startEmbeddedHbase() {
        try {
            testUtil.startMiniCluster();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void stopEmbeddedDatabase(){
        try {
            testUtil.cleanupTestDir();
            testUtil.shutdownMiniCluster();
        } catch (IOException e) {
            e.printStackTrace();  
        }
    }

    public static Hashtable<String,String> tableData(String tableName){
        
        Hashtable<String, String> hashtable = new Hashtable<String, String>();
        try {
            HTableDescriptor tableDescriptor = testUtil.getHBaseAdmin().getTableDescriptor(tableName.getBytes());
            hashtable.put("tableName",tableDescriptor.getNameAsString());
            hashtable.put("families",new Integer(tableDescriptor.getColumnFamilies().length).toString());
            return hashtable;
        } catch (IOException e) {
            e.printStackTrace();
            return hashtable;
        }
    }

    public static HBaseAdmin getAdmin(){
        try {
            return testUtil.getHBaseAdmin();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }



}
