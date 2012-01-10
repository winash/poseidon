package poseidon.tests;

import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.Vertex;
import poseidon.HVertex;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author Ashwin
 */
public class HIndexTest extends HBaseTestBase {

    @Test
    public void shouldAddVertexDataToIndex() {

        Vertex vertex1 = graph.addVertex(null);
        vertex1.setProperty("name","ashwin");
        vertex1.setProperty("king","george");
        vertex1.setProperty("lame","duck");

        Index<HVertex> vertexIndex = graph.getIndex("lookowtvertex", HVertex.class);

        CloseableSequence<HVertex> set = vertexIndex.get("name", "ashwin");
        HVertex hVertex = set.iterator().next();
        assertThat(hVertex.getPropertyKeys().size(),is(4));
        assertThat((String)hVertex.getProperty("name"),is("ashwin"));
        assertThat((String)hVertex.getProperty("lame"),is("duck"));

    }


}