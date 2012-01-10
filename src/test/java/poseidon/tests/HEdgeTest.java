package poseidon.tests;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author Ashwin
 */
public class HEdgeTest extends HBaseTestBase{


    @Test
    public void shouldGetLabelForEdge() {
        Vertex vertex1 = graph.addVertex(null);
        Vertex vertex2 = graph.addVertex(null);
        Edge edge = graph.addEdge(null, vertex1, vertex2, "knows");
        String label = edge.getLabel();
        assertThat(label, is("knows"));

    }

    @Test
    public void shouldGetPropertyKeys() {
        Vertex vertex1 = graph.addVertex(null);
        Vertex vertex2 = graph.addVertex(null);
        Edge edge = graph.addEdge(null, vertex1, vertex2, "eats");
        edge.setProperty("fname", "pyarelal");
        edge.setProperty("lname", "jaanidushman");
        assertThat(edge.getPropertyKeys().size(), is(3));

    }

    @Test
    public void shouldAddInAndOutVertices() {
        Vertex vertex1 = graph.addVertex(null);
        Vertex vertex2 = graph.addVertex(null);
        Edge edge = graph.addEdge(null, vertex1, vertex2, "related");
        Vertex inVertex = edge.getInVertex();
        Vertex outVertex = edge.getOutVertex();
        byte[] inVertexId = (byte[]) inVertex.getId();
        byte[] outVertexId = (byte[]) outVertex.getId();
        assertThat(inVertexId, is((byte[]) vertex2.getId()));
        assertThat(outVertexId, is((byte[]) vertex1.getId()));
    }

    @Test
    public void shouldGetPropertyFromEdge() {
        Vertex vertex1 = graph.addVertex(null);
        Vertex vertex2 = graph.addVertex(null);
        Edge edge = graph.addEdge(null, vertex1, vertex2, "eats");
        edge.setProperty("fname", "pyarelal");
        edge.setProperty("lname", "jaanidushman");
        String prop = (String) edge.getProperty("fname");
        String prop2 = (String) edge.getProperty("lname");
        assertThat(prop, is("pyarelal"));
        assertThat(prop2, is("jaanidushman"));
    }

    @Test
    public void shouldRemovePropertyFromEdge() {
        Vertex vertex1 = graph.addVertex(null);
        Vertex vertex2 = graph.addVertex(null);
        Edge edge = graph.addEdge(null, vertex1, vertex2, "eats");
        edge.setProperty("fname", "pyarelal");
        edge.setProperty("lname", "jaanidushman");
        assertThat(edge.getPropertyKeys().size(), is(3));

        edge.removeProperty("fname");
        edge.removeProperty("lname");
        assertThat(edge.getPropertyKeys().size(), is(1));
    }


}
