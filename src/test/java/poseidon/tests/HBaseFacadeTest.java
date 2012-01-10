package poseidon.tests;

import poseidon.hbase.HBaseFacade;
import org.junit.Test;

import java.util.Hashtable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;


/**
 * @author Ashwin
 */
public class HBaseFacadeTest extends HBaseTestBase{


    @Test
    public void shouldCreateVertexTable(){
        new HBaseFacade("localhost", "21818", "lookowt", true);
        Hashtable<String,String> hashtable = TestUtil.tableData("lookowt._.vertex");
        assertThat(hashtable.get("tableName"),is("lookowt._.vertex"));
        assertThat(hashtable.get("families"),is("3"));
    }


    @Test
    public void shouldCreateEdgeTable(){
        new HBaseFacade("localhost", "21818", "lookowt", true);
        Hashtable<String,String> hashtable = TestUtil.tableData("lookowt._.edge");
        assertThat(hashtable.get("tableName"),is("lookowt._.edge"));
        assertThat(hashtable.get("families"),is("3"));
    }

    @Test
    public void shouldCreateIndexTables(){
        new HBaseFacade("localhost", "21818", "lookowt", true);
        Hashtable<String,String> hashtable = TestUtil.tableData("lookowt._.v_index");
        Hashtable<String,String> hashtable2 = TestUtil.tableData("lookowt._.e_index");
        assertThat(hashtable.get("tableName"),is("lookowt._.v_index"));
        assertThat(hashtable2.get("tableName"),is("lookowt._.e_index"));
        assertThat(hashtable.get("families"),is("2"));
        assertThat(hashtable2.get("families"),is("2"));
    }
   
}
