package poseidon.tests;

import poseidon.HGraph;
import org.junit.After;
import org.junit.Before;

/**
 * @author Ashwin
 */
public abstract class HBaseTestBase {

    protected HGraph graph;

    @Before
    public void setUp() {
        TestUtil.startEmbeddedHbase();
        this.graph = new HGraph("localhost", "21818", "lookowt", true);
    }

    @After
    public void tearDown() {
        TestUtil.stopEmbeddedDatabase();
    }


}
