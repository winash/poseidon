package poseidon.tests;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import org.junit.Test;

import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author Ashwin
 */
public class HVertexTest extends HBaseTestBase {

    @Test
    public void shouldAddPropertiesToVertexAndCount() {
        Vertex vertex = graph.addVertex(null);
        vertex.setProperty("name", "ashwin");
        vertex.setProperty("foo", "bar");

        assertThat(vertex.getPropertyKeys().size(), is(3));
    }

    @Test
    public void shouldGetPropertiesFromVertex() {
        Vertex vertex = graph.addVertex(null);
        vertex.setProperty("name", "ashwin");
        vertex.setProperty("foo", "bar");

        assertThat((String)vertex.getProperty("name"), is("ashwin"));
        assertThat((String)vertex.getProperty("foo"), is("bar"));
    }


    @Test
    public void shouldRemovePropertyFromVertex() {
        Vertex vertex = graph.addVertex(null);
        vertex.setProperty("name", "ashwin");
        vertex.setProperty("foo", "bar");
        vertex.removeProperty("name");
        vertex.removeProperty("foo");

        assertThat(vertex.getPropertyKeys().size(), is(1));
    }

    @Test
    public void shouldFetchOutAndInEdgesForVertex() {
        Vertex outVertex = graph.addVertex(null);
        Vertex inVertex = graph.addVertex(null);
        Edge edge = graph.addEdge(null, outVertex, inVertex, "related");
        Edge edge2 = graph.addEdge(null, outVertex, inVertex, "bar");
        ArrayList<Edge> outEdgeArrayList = (ArrayList<Edge>) outVertex.getOutEdges();
        ArrayList<Edge> inEdgeArrayList = (ArrayList<Edge>) inVertex.getInEdges();
        assertThat(outEdgeArrayList.size(), is(2));
        assertThat(inEdgeArrayList.size(), is(2));
    }

    @Test
    public void shouldLoadEdgesBasedOnLabel() {

        Vertex outVertex = graph.addVertex(null);
        Vertex inVertex = graph.addVertex(null);
        Edge edge = graph.addEdge(null, outVertex, inVertex, "related");
        Edge edge2 = graph.addEdge(null, outVertex, inVertex, "bar");
        ArrayList<Edge> outEdgeArrayList = (ArrayList<Edge>) outVertex.getOutEdges("related");
        ArrayList<Edge> inEdgeArrayList = (ArrayList<Edge>) inVertex.getInEdges("bar");
        assertThat(outEdgeArrayList.size(), is(1));
        assertThat(inEdgeArrayList.size(), is(1));


    }

}