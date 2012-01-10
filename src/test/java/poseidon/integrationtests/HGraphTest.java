package poseidon.integrationtests;

import com.tinkerpop.blueprints.pgm.*;
import com.tinkerpop.blueprints.pgm.impls.GraphTest;
import com.tinkerpop.blueprints.pgm.util.graphml.GraphMLReaderTestSuite;
import poseidon.tests.TestUtil;
import poseidon.HGraph;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.lang.reflect.Method;

/**
 * @author Ashwin
 */
public class HGraphTest extends GraphTest{
    private Graph graph;


    @Before
    public void setUp() {
        TestUtil.startEmbeddedHbase();
        this.graph = new HGraph("localhost", "21818", "lookowt", true);
    }

    @After
    public void tearDown() {
        TestUtil.stopEmbeddedDatabase();
    }
    

    public HGraphTest() {
        this.allowsDuplicateEdges = true;
        this.allowsSelfLoops = true;
        this.isPersistent = true;
        this.isRDFModel = false;
        this.supportsVertexIteration = true;
        this.supportsEdgeIteration = true;
        this.supportsVertexIndex = true;
        this.supportsEdgeIndex = true;
        this.ignoresSuppliedIds = true;
        this.supportsTransactions = false;
    }

    @Override
    public Graph getGraphInstance() {
        HGraph hgraph = (HGraph) this.graph;
        hgraph.startUp();
        return hgraph;
    }

    public void testVertexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexTestSuite(this));
        printTestPerformance("VertexTestSuite", this.stopWatch());
    }

    public void testEdgeTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new EdgeTestSuite(this));
        printTestPerformance("EdgeTestSuite", this.stopWatch());
    }

    public void testGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphTestSuite(this));
        printTestPerformance("GraphTestSuite", this.stopWatch());
    }

    public void testAutomaticIndexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new AutomaticIndexTestSuite(this));
        printTestPerformance("AutomaticIndexTestSuite", this.stopWatch());
    }
    

    public void testGraphMLReaderTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphMLReaderTestSuite(this));
        printTestPerformance("GraphMLReaderTestSuite", this.stopWatch());
    }


    private String getWorkingDirectory() {
        String directory = System.getProperty("HGraphDirectory");
        if (directory == null) {
            if (System.getProperty("os.name").toUpperCase().contains("WINDOWS"))
                directory = "C:/temp/hGraph_test";
            else
                directory = "/tmp/hGraph_test";
        }
        return directory;
    }

    @Override
    public void doTestSuite(final TestSuite testSuite) throws Exception {
        String doTest = System.getProperty("testHGraph");
        if (doTest == null || doTest.equals("true")) {
            String directory = System.getProperty("HGraphDirectory");
            if (directory == null)
                directory = this.getWorkingDirectory();
            deleteDirectory(new File(directory));
            for (Method method : testSuite.getClass().getDeclaredMethods()) {
                if (method.getName().startsWith("test") && !method.getName().equals("testAddingSelfLoops") && !method.getName().equals("testRemoveSelfLoops") && !method.getName().equals("testEdgeIterator") && !method.getName().equals("testGraphDataPersists") && !method.getName().equals("testAutomaticIndexKeysPersistent") && !method.getName().equals("testEdgeLabelIndexing")) {
                    System.out.println("Testing " + method.getName() + "...");
                    method.invoke(testSuite);
                    deleteDirectory(new File(directory));
                }
            }
        }
    }





}
