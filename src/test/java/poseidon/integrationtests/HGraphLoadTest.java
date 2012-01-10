package poseidon.integrationtests;

import com.tinkerpop.blueprints.pgm.Graph;
import com.tinkerpop.blueprints.pgm.TestSuite;
import com.tinkerpop.blueprints.pgm.util.graphml.GraphMLReader;
import poseidon.tests.TestUtil;
import poseidon.HGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 * @author Ashwin
 */
public class HGraphLoadTest extends TestSuite {

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

    @Test
    public void testLoadingLargeGraph() throws XMLStreamException, FileNotFoundException {

        final GraphMLReader graphMLReader = new GraphMLReader(graph);
        graphMLReader.inputGraph(new FileInputStream(new File("hierarchy.graphml")));
        printPerformance(graph.toString(), null, "graph-example-1 loaded", this.stopWatch());
    }


}
