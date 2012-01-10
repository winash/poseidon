package poseidon;

import com.tinkerpop.blueprints.pgm.*;
import com.tinkerpop.blueprints.pgm.util.AutomaticIndexHelper;
import poseidon.hbase.HBaseFacade;
import poseidon.util.HBaseUtils;
import poseidon.util.LogUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

import static poseidon.Constants.*;

/**
 * @author Ashwin
 */
public class HGraph implements Graph, IndexableGraph {


    private static Logger LOG = LogUtil.getLoggerFor(HGraph.class);

    private HBaseFacade underlying;

    private final Map<String, HIndex> indices = new HashMap<String, HIndex>();

    private String graphName;
    


    public HGraph(String zookeeperQuorum, String port, String graphName, boolean createNew) {
        this.graphName = graphName;
        this.underlying = new HBaseFacade(zookeeperQuorum, port, graphName, createNew);
        createIndices();
    }

    private void createIndices() {
        this.createAutomaticIndex(Index.VERTICES, HVertex.class, null);
        this.createAutomaticIndex(Index.EDGES, HEdge.class, null);
    }


    @Override
    public <T extends Element> Index<T> createManualIndex(String s, Class<T> tClass) {
        throw new NotImplementedException();
    }

    @Override
    public <T extends Element> AutomaticIndex<T> createAutomaticIndex(String indexName, Class<T> tClass, Set<String> indexKeys) {
        if (this.indices.containsKey(indexName))
            throw new RuntimeException(new StringBuilder().append("index ").append(indexName).append("already exists").toString());
        final HIndex hIndex = new HIndex(this, indexName, tClass, indexKeys, underlying);
        this.indices.put(indexName, hIndex);
        return hIndex;
    }

    @Override
    public <T extends Element> Index<T> getIndex(String indexName, Class<T> tClass) {
        return this.indices.get(indexName);
    }

    @Override
    public Iterable<Index<? extends Element>> getIndices() {
        final List<Index<? extends Element>> list = new ArrayList<Index<? extends Element>>();
        for (Index index : indices.values())
            list.add(index);
        return list;
    }

    @Override
    public void dropIndex(String key) {
        this.indices.remove(key);
    }

    @Override
    public Vertex addVertex(Object o) {
        LOG.info("Adding Vertex ");
        byte[] id = HBaseUtils.newIdentifier();
        HVertex hVertex = new HVertex(this);
        hVertex.setId(id);
        Put put = new Put(id);
        put.add(Bytes.toBytes(VERTEX + PROPERTIES), Bytes.toBytes(CREATED), Bytes.toBytes(new Date().toString()));
        underlying.putInVertexTable(put);
        return hVertex;
    }

    @Override
    public Vertex getVertex(Object id) {
        byte[] identifier = (byte[]) id;
        Get get = new Get(identifier);
        Result result = underlying.getFromVertexTable(get);
        if (result.isEmpty())
            return null;
        HVertex hVertex = new HVertex(this);
        hVertex.setId(identifier);
        return hVertex;
    }

    @Override
    public void removeVertex(Vertex vertex) {
        //load both incoming and outgoing vertices, delete references across tables
        ArrayList<Edge> inEdges = (ArrayList<Edge>) vertex.getInEdges();
        ArrayList<Edge> outEdges = (ArrayList<Edge>) vertex.getOutEdges();

        //for each edge, remove references to vertex
        HTable edgeTable = underlying.getEdgeTable();
        for (Edge edge : inEdges) {
            this.removeEdge(edge);
        }

        for (Edge edge : outEdges) {
            this.removeEdge(edge);
        }
        AutomaticIndexHelper.removeElement(this, vertex);
        Delete delete = new Delete((byte[]) vertex.getId());
        try {
            underlying.getVertexTable().delete(delete);
        } catch (IOException e) {
            LOG.info("could not get delete vertex");
            e.printStackTrace();
            return;
        }
    }

    @Override
    public Iterable<Vertex> getVertices() {
        HTable vertexTable = underlying.getVertexTable();
        ArrayList<Vertex> vertices = new ArrayList<Vertex>();
        Scan scan = new Scan();
        ResultScanner scanner = null;
        try {
            scanner = vertexTable.getScanner(scan);
            Result res;
            while ((res = scanner.next()) != null) {
                HVertex hVertex = new HVertex(this);
                byte[] row = res.getRow();
                hVertex.setId(row);
                vertices.add(hVertex);
            }
        } catch (IOException e) {
            LOG.info("could not get scanner for vertex table");
            e.printStackTrace();
            return new ArrayList<Vertex>();
        }

        return vertices;
    }

    @Override
    public Edge addEdge(Object o, Vertex outVertex, Vertex inVertex, String label) {
        LOG.info("Adding Edge ");
        byte[] id = HBaseUtils.newIdentifier();
        byte[] v1Id = (byte[]) outVertex.getId();
        byte[] v2Id = (byte[]) inVertex.getId();

        //Build edge table entry
        HEdge hEdge = new HEdge(this);
        hEdge.setId(id);
        Put put = new Put(id);
        put.add(Bytes.toBytes(OUTVERTICES), HBaseUtils.newIdentifier(), v1Id);
        put.add(Bytes.toBytes(INVERTICES), HBaseUtils.newIdentifier(), v2Id);
        put.add(Bytes.toBytes(EDGE + PROPERTIES), Bytes.toBytes(LABEL), Bytes.toBytes(label));

        for(Index index : this.getIndices()){
            final HIndex hIndex = (HIndex) index;
            hIndex.autoUpdate(AutomaticIndex.LABEL, label, null, hEdge);
        }

        underlying.putInEdgeTable(put);
        //build entries for both vertices
        put = new Put((byte[]) outVertex.getId());
        put.add(Bytes.toBytes(OUTEDGES), HBaseUtils.newIdentifier(), id);
        underlying.putInVertexTable(put);

        put = new Put((byte[]) inVertex.getId());
        put.add(Bytes.toBytes(INEDGES), HBaseUtils.newIdentifier(), id);
        underlying.putInVertexTable(put);

        return hEdge;
    }

    @Override
    public Edge getEdge(Object id) {
        byte[] identifier = (byte[]) id;
        Get get = new Get(identifier);
        Result result = null;
        try {
            result = underlying.getEdgeTable().get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (result.isEmpty())
            return null;
        HEdge hEdge = new HEdge(this);
        hEdge.setId(identifier);
        return hEdge;
    }

    @Override
    public void removeEdge(Edge edge) {
        //delete all references from the vertices table
        Vertex inVertex = edge.getInVertex();
        Vertex outVertex = edge.getOutVertex();

        //load row for each vertex, then delete the edges from the edge collection
        HVertex hInVertex = (HVertex) inVertex;
        HVertex hOutVertex = (HVertex) outVertex;

        hInVertex.removeFromInEdgesList((byte[]) edge.getId());
        hOutVertex.removeFromOutEdgesList((byte[]) edge.getId());

        AutomaticIndexHelper.removeElement(this, edge);

        Delete delete = new Delete((byte[]) edge.getId());
        try {
            underlying.getEdgeTable().delete(delete);
        } catch (IOException e) {
            LOG.info("could not get delete edge");
            e.printStackTrace();
            return;
        }

    }

    @Override
    public Iterable<Edge> getEdges() {
        HTable edgeTable = underlying.getEdgeTable();
        ArrayList<Edge> edges = new ArrayList<Edge>();
        Scan scan = new Scan();
        ResultScanner scanner = null;
        try {
            scanner = edgeTable.getScanner(scan);
            Result res;
            while ((res = scanner.next()) != null) {
                HEdge hEdge = new HEdge(this);
                byte[] row = res.getRow();
                hEdge.setId(row);
                edges.add(hEdge);
            }
        } catch (IOException e) {
            LOG.info("could not get scanner for vertex table");
            e.printStackTrace();
            return new ArrayList<Edge>();
        }

        return edges;
    }

    @Override
    public void clear() {
        underlying.clearAll();
    }

    @Override
    public void shutdown() {
        clearExtraIndices();
        underlying.shutDown();
    }

    private void clearExtraIndices() {
        final Set<String> stringSet = this.indices.keySet();
        List<String> keys  = new ArrayList<String>();
        for(String key : stringSet){
           if(Index.VERTICES.equals(key) || Index.EDGES.equals(key))
              continue;
            keys.add(key);
        }

        for(String key : keys)
            indices.remove(key);
    }

    public void checkAndPutVertex(byte[] id, byte[] bytes, byte[] key, Put put) {
        try {
            underlying.getVertexTable().put(put);
        } catch (IOException e) {
            LOG.info("could not add update column family");
            e.printStackTrace();
        }
    }

    public HTable getEdgeTable() {
        return underlying.getEdgeTable();
    }

    public HTable getVertexTable() {
        return underlying.getVertexTable();
    }

    public void startUp(){
        underlying.startUp();
    }


}

