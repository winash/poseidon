package poseidon;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.Vertex;
import poseidon.serialize.ByteArraySerializer;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

import static poseidon.Constants.*;


/**
 * @author Ashwin
 */
public class HVertex extends HElement implements Vertex {

    public HVertex(HGraph hGraph) {
        this.hGraph = hGraph;
    }

    public Iterable<Edge> getOutEdges() {
        Get get = new Get(id);
        HTable vertexTable = hGraph.getVertexTable();
        Result result = null;
        try {
            result = vertexTable.get(get);
        } catch (IOException e) {
            LOG.info("Error while fetching edges");
            e.printStackTrace();
            return null;
        }

        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes(OUTEDGES));
        ArrayList<Edge> edgeArrayList = new ArrayList<Edge>();
        for (byte[] value : map.values()) {
            HEdge hEdge = new HEdge(hGraph);
            hEdge.setId(value);
            edgeArrayList.add(hEdge);
        }
        return edgeArrayList;
    }

    public Iterable<Edge> getInEdges() {
        Get get = new Get(id);
        HTable vertexTable = hGraph.getVertexTable();
        Result result = null;
        try {
            result = vertexTable.get(get);
        } catch (IOException e) {
            LOG.info("Error while fetching edges");
            e.printStackTrace();
            return null;
        }

        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes(INEDGES));
        ArrayList<Edge> edgeArrayList = new ArrayList<Edge>();
        for (byte[] value : map.values()) {
            HEdge hEdge = new HEdge(hGraph);
            hEdge.setId(value);
            edgeArrayList.add(hEdge);
        }
        return edgeArrayList;
    }

    @Override
    public Iterable<Edge> getOutEdges(String... labels) {
        List<String> labelList = Arrays.asList(labels);
        ArrayList<Edge> edgeArrayList = new ArrayList<Edge>();
        Iterable<Edge> outEdges = this.getOutEdges();
        if (labelList.size() == 0)
            return outEdges;
        for (Edge edge : outEdges) {
            if (labelList.contains(edge.getLabel()))
                edgeArrayList.add(edge);
        }
        return edgeArrayList;
    }

    @Override
    public Iterable<Edge> getInEdges(String... labels) {
        List<String> labelList = Arrays.asList(labels);
        ArrayList<Edge> edgeArrayList = new ArrayList<Edge>();
        Iterable<Edge> inEdges = this.getInEdges();
        if (labelList.size() == 0)
            return inEdges;
        for (Edge edge : inEdges) {
            if (labelList.contains(edge.getLabel()))
                edgeArrayList.add(edge);
        }
        return edgeArrayList;
    }

    @Override
    public Object getProperty(String key) {
        Get get = new Get(id);
        HTable vertexTable = hGraph.getVertexTable();
        Result result = null;
        try {
            result = vertexTable.get(get);
        } catch (IOException e) {
            LOG.info("Error while fetching property");
            e.printStackTrace();
            return null;
        }

        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes(VERTEX + PROPERTIES));
        byte[] data = map.get(Bytes.toBytes(key));
        if(null == data)
            return null;
        return ByteArraySerializer.toObject(data);
    }

    @Override
    public Set<String> getPropertyKeys() {
        Get get = new Get(id);
        HTable vertexTable = hGraph.getVertexTable();
        Result result = null;
        try {
            result = vertexTable.get(get);
        } catch (IOException e) {
            LOG.info("Error while fetching property keys");
            e.printStackTrace();
            return null;
        }

        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes(VERTEX + PROPERTIES));
        HashSet<String> keys = new HashSet<String>();
        for (byte[] key : map.keySet()) {
            String inKey = new String(key);
            if(inKey.equals(CREATED))
                continue;
            keys.add(inKey);
        }
        return keys;
    }

    @Override
    public void setProperty(String key, Object value) {
        if(key.equals(ID))
            throw new RuntimeException("Property 'id' cannot be added");
        byte[] byteValue = ByteArraySerializer.fromObject(value);
        final Object oldValue = this.getProperty(key);
        Put put = new Put(id);
        put.add(Bytes.toBytes(VERTEX + PROPERTIES), Bytes.toBytes(key), byteValue);
        hGraph.checkAndPutVertex(id, Bytes.toBytes(VERTEX + PROPERTIES), Bytes.toBytes(key), put);
        Iterable<Index<? extends Element>> indices = this.hGraph.getIndices();
        for (Index index : indices) {
            HIndex hIndex = (HIndex) index;
            hIndex.autoUpdate(key, value, oldValue, this);
        }

    }

    @Override
    public Object removeProperty(String key) {
        Object oldValue = this.getProperty(key);
        if(null == oldValue)
            return null;
        Delete delete = new Delete(id);
        delete.deleteColumns(Bytes.toBytes(VERTEX + PROPERTIES), Bytes.toBytes(key));
        try {
            hGraph.getVertexTable().delete(delete);
        } catch (IOException e) {
            LOG.info("Error while removing property");
            e.printStackTrace();
            return null;
        }

        Iterable<Index<? extends Element>> indices = this.hGraph.getIndices();
        for (Index index : indices) {
            HIndex hIndex = (HIndex) index;
            hIndex.autoRemove(key, oldValue, this);
        }

        return oldValue;
    }

    public void removeFromOutEdgesList(byte[] edgeId) {
        Get get = new Get(id);
        HTable vertexTable = hGraph.getVertexTable();
        Result result = null;
        try {
            result = vertexTable.get(get);
        } catch (IOException e) {
            LOG.info("Error while fetching edges");
            e.printStackTrace();
            return;
        }

        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes(OUTEDGES));
        Set<Map.Entry<byte[], byte[]>> entries = map.entrySet();
        byte[] found = null;
        for (Map.Entry<byte[], byte[]> entry : entries) {
            if (entry.getValue().equals(edgeId)) ;
            found = entry.getKey();
        }

        if (found == null)
            return;

        Delete delete = new Delete(id);
        delete.deleteColumns(Bytes.toBytes(OUTEDGES), found);

        try {
            vertexTable.delete(delete);
        } catch (IOException e) {
            LOG.info("Error while deleting outgoing Edge");
            e.printStackTrace();
        }
    }

    public void removeFromInEdgesList(byte[] edgeId) {
        Get get = new Get(id);
        HTable vertexTable = hGraph.getVertexTable();
        Result result = null;
        try {
            result = vertexTable.get(get);
        } catch (IOException e) {
            LOG.info("Error while fetching edges");
            e.printStackTrace();
            return;
        }

        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes(INEDGES));
        Set<Map.Entry<byte[], byte[]>> entries = map.entrySet();
        byte[] found = null;
        for (Map.Entry<byte[], byte[]> entry : entries) {
            if (entry.getValue().equals(edgeId)) ;
            found = entry.getKey();
        }

        if (found == null)
            return;

        Delete delete = new Delete(id);
        delete.deleteColumns(Bytes.toBytes(INEDGES), found);

        try {
            vertexTable.delete(delete);
        } catch (IOException e) {
            LOG.info("Error while deleting incoming Edge");
            e.printStackTrace();
        }
    }

}
