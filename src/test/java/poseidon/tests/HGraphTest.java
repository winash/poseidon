package poseidon.tests;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import poseidon.util.HBaseUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.NavigableMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;


/**
 * @author Ashwin
 */
public class HGraphTest extends HBaseTestBase {


    @Test
    public void shouldCreateVertex() throws IOException {
        Vertex vertex = graph.addVertex(null);
        byte[] id = (byte[]) vertex.getId();
        assertThat(id.length, is(16));
    }

    @Test
    public void shouldAllowAddingVertexProperties() throws IOException {
        Vertex vertex = graph.addVertex(null);
        byte[] id = (byte[]) vertex.getId();
        vertex.setProperty("name", "ashwin");
        vertex.setProperty("home", "bangalore");
        vertex.setProperty("iq", "200");
        assertThat(id.length, is(16));

        HBaseAdmin hBaseAdmin = TestUtil.getAdmin();
        Get get = new Get(id);
        HTable hTable = new HTable(hBaseAdmin.getConfiguration(), "lookowt._.vertex");
        Result result = hTable.get(get);
        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes("vertexproperties"));

        assertThat(map.size(), is(4));
        assertThat(new String(map.get(Bytes.toBytes("name"))), is("ashwin"));
        assertThat(new String(map.get(Bytes.toBytes("home"))), is("bangalore"));
        assertThat(new String(map.get(Bytes.toBytes("iq"))), is("200"));

    }

    @Test
    public void shouldAddEdgeBetweenExistingVertices() throws IOException {
        Vertex vertex1 = graph.addVertex(null);
        Vertex vertex2 = graph.addVertex(null);
        Edge edge = graph.addEdge(null, vertex1, vertex2, "knows");
        byte[] id = (byte[]) edge.getId();
        assertThat(id.length, is(16));

        HBaseAdmin hBaseAdmin = TestUtil.getAdmin();

        Get get = new Get(id);
        HTable hTable = new HTable(hBaseAdmin.getConfiguration(), "lookowt._.edge");
        Result result = hTable.get(get);
        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes("edgeproperties"));
        NavigableMap<byte[], byte[]> outVertices = result.getFamilyMap(Bytes.toBytes("outvertices"));
        NavigableMap<byte[], byte[]> inVertices = result.getFamilyMap(Bytes.toBytes("invertices"));
        assertThat(map.size(), is(1));
        assertThat(outVertices.size(), is(1));
        assertThat(inVertices.size(), is(1));
        assertThat(HBaseUtils.getFirstValueFromMap(outVertices), is((byte[]) vertex1.getId()));
        assertThat(HBaseUtils.getFirstValueFromMap(inVertices), is((byte[]) vertex2.getId()));

        get = new Get((byte[]) vertex1.getId());
        HTable vTable = new HTable(hBaseAdmin.getConfiguration(), "lookowt._.vertex");
        Result result1 = vTable.get(get);
        NavigableMap<byte[], byte[]> v1map = result1.getFamilyMap(Bytes.toBytes("outedges"));

        get = new Get((byte[]) vertex2.getId());
        Result result2 = vTable.get(get);
        NavigableMap<byte[], byte[]> v2map = result2.getFamilyMap(Bytes.toBytes("inedges"));

        assertThat(v1map.keySet().size(), is(1));
        assertThat(HBaseUtils.getFirstValueFromMap(v1map), is((byte[]) edge.getId()));
        assertThat(v2map.keySet().size(), is(1));
        assertThat(HBaseUtils.getFirstValueFromMap(v2map), is((byte[]) edge.getId()));


    }

    @Test
    public void shouldLoadAllVerticesForGraph() {
        graph.addVertex(null);
        graph.addVertex(null);
        graph.addVertex(null);

        Iterable<Vertex> iterable = graph.getVertices();
        ArrayList<Vertex> vertexArrayList = (ArrayList<Vertex>) iterable;
        assertThat(vertexArrayList.size(), is(3));

    }

    @Test
    public void shouldLoadAllEdgesForGraph() {
        Vertex vertex1 = graph.addVertex(null);
        Vertex vertex2 = graph.addVertex(null);
        graph.addVertex(null);
        ArrayList<Edge> edges = (ArrayList<Edge>) graph.getEdges();
        assertThat(edges.size(), is(0));

        graph.addEdge(null, vertex1, vertex2, "location");
        graph.addEdge(null, vertex2, vertex2, "designation");
        edges = (ArrayList<Edge>) graph.getEdges();
        assertThat(edges.size(), is(2));

    }

    @Test
    public void shouldRemoveVertex() {
        Vertex vertex1 = graph.addVertex(null);
        graph.addVertex(null);

        Iterable<Vertex> iterable = graph.getVertices();
        ArrayList<Vertex> vertexArrayList = (ArrayList<Vertex>) iterable;
        assertThat(vertexArrayList.size(), is(2));

        graph.removeVertex(vertex1);
        iterable = graph.getVertices();
        vertexArrayList = (ArrayList<Vertex>) iterable;
        assertThat(vertexArrayList.size(), is(1));

    }


    @Test
    public void shouldRemoveEdges() {
        Vertex vertex1 = graph.addVertex(null);
        Vertex vertex2 = graph.addVertex(null);
        Vertex vertex3 = graph.addVertex(null);

        Edge edge1 = graph.addEdge(null, vertex1, vertex2, "location");
        Edge edge2 = graph.addEdge(null, vertex2, vertex3, "designation");
        graph.addEdge(null, vertex1, vertex3, "blah");
        ArrayList<Edge> edges = (ArrayList<Edge>) graph.getEdges();
        assertThat(edges.size(), is(3));
        ArrayList<Edge> edgeArrayList = (ArrayList<Edge>) vertex1.getOutEdges();
        assertThat(edgeArrayList.size(), is(2));

        edgeArrayList = (ArrayList<Edge>) vertex3.getInEdges();
        assertThat(edgeArrayList.size(), is(2));

        graph.removeEdge(edge1);
        graph.removeEdge(edge2);
        edgeArrayList = (ArrayList<Edge>) vertex1.getOutEdges();
        assertThat(edgeArrayList.size(), is(1));

        edgeArrayList = (ArrayList<Edge>) vertex3.getInEdges();
        assertThat(edgeArrayList.size(), is(1));

        edgeArrayList = (ArrayList<Edge>) vertex2.getOutEdges();
        assertThat(edgeArrayList.size(), is(0));

        edges = (ArrayList<Edge>) graph.getEdges();
        assertThat(edges.size(), is(1));


    }


    @Test
    public void shouldRemoveVertexAndUpdateEdgeTable() {
        Vertex vertex1 = graph.addVertex(null);
        Vertex vertex2 = graph.addVertex(null);
        Vertex vertex3 = graph.addVertex(null);
        Vertex vertex4 = graph.addVertex(null);

        Edge edge1 = graph.addEdge(null, vertex1, vertex2, "location");
        Edge edge2 = graph.addEdge(null, vertex2, vertex3, "designation");
        Edge edge3 = graph.addEdge(null, vertex1, vertex3, "blah");
        Edge edge4 = graph.addEdge(null, vertex3, vertex4, "blah");

        graph.removeVertex(vertex1);
        graph.removeVertex(vertex2);
        ArrayList<Edge> edges = (ArrayList<Edge>) graph.getEdges();
        assertThat(edges.size(),is(1));
    }


}


