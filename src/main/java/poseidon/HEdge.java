package poseidon;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.Vertex;
import poseidon.serialize.ByteArraySerializer;
import poseidon.util.HBaseUtils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;

import static poseidon.Constants.*;


/**
 * @author Ashwin
 */
public class HEdge extends HElement implements Edge {

    public HEdge(HGraph hGraph) {
        this.hGraph = hGraph;
    }

    @Override
    public Vertex getOutVertex() {
        Get get = new Get(id);
        HTable edgeTable = hGraph.getEdgeTable();
        Result result = null;
        try {
            result = edgeTable.get(get);
        } catch (IOException e) {
            LOG.info("Error while fetching out vertex");
            e.printStackTrace();
            return null;
        }

        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes(OUTVERTICES));
        byte[] identifier = HBaseUtils.getFirstValueFromMap(map);
        if (null == identifier)
            return null;
        HVertex hVertex = new HVertex(hGraph);
        hVertex.setId(identifier);
        return hVertex;
    }

    @Override
    public Vertex getInVertex() {
        Get get = new Get(id);
        HTable edgeTable = hGraph.getEdgeTable();
        Result result = null;
        try {
            result = edgeTable.get(get);
        } catch (IOException e) {
            LOG.info("Error while fetching in vertex");
            e.printStackTrace();
            return null;
        }

        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes(INVERTICES));
        byte[] identifier = HBaseUtils.getFirstValueFromMap(map);
        if (null == identifier)
            return null;
        HVertex hVertex = new HVertex(hGraph);
        hVertex.setId(identifier);
        return hVertex;
    }

    @Override
    public String getLabel() {
        Get get = new Get(id);
        HTable edgeTable = hGraph.getEdgeTable();
        Result result = null;
        try {
            result = edgeTable.get(get);
        } catch (IOException e) {
            LOG.info("Error while fetching label");
            e.printStackTrace();
            return null;
        }

        byte[] data = result.getValue(Bytes.toBytes(EDGE + PROPERTIES), Bytes.toBytes(LABEL));
        return new String(data);
    }


    @Override
    public Object getProperty(String key) {
        Get get = new Get(id);
        HTable edgeTable = hGraph.getEdgeTable();
        Result result = null;
        try {
            result = edgeTable.get(get);
        } catch (IOException e) {
            LOG.info("Error while fetching property");
            e.printStackTrace();
            return null;
        }

        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes(EDGE + PROPERTIES));
        byte[] data = map.get(Bytes.toBytes(key));
        if(null == data)
            return null;
        return ByteArraySerializer.toObject(data);
    }

    @Override
    public Set<String> getPropertyKeys() {
        Get get = new Get(id);
        HTable edgeTable = hGraph.getEdgeTable();
        Result result = null;
        try {
            result = edgeTable.get(get);
        } catch (IOException e) {
            LOG.info("Error while fetching property keys");
            e.printStackTrace();
            return null;
        }

        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes(EDGE + PROPERTIES));
        HashSet<String> keys = new HashSet<String>();
        for (byte[] key : map.keySet()) {
            String inKey = new String(key);
            if(inKey.equals(LABEL))
                continue;
            keys.add(new String(key));
        }
        return keys;
    }

    @Override
    public void setProperty(String key, Object value) {
        if(key.equals(ID) || key.equals(LABEL))
            throw new RuntimeException("Property 'id' or 'label' cannot be added");
        byte[] byteValue = ByteArraySerializer.fromObject(value);
        Put put = new Put(id);
        put.add(Bytes.toBytes(EDGE + PROPERTIES), Bytes.toBytes(key), byteValue);
        final Object oldValue = this.getProperty(key);
        try {
            hGraph.getEdgeTable().put(put);
        } catch (IOException e) {
            LOG.info("Error while setting property");
            e.printStackTrace();
            return;
        }
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
        delete.deleteColumns(Bytes.toBytes(EDGE + PROPERTIES), Bytes.toBytes(key));
        try {
            hGraph.getEdgeTable().delete(delete);
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

    public byte[] getOutVertexId() {
        Get get = new Get(id);
        HTable edgeTable = hGraph.getEdgeTable();
        Result result = null;
        try {
            result = edgeTable.get(get);
        } catch (IOException e) {
            LOG.info("Error while fetching out vertex");
            e.printStackTrace();
            return null;
        }

        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes(OUTVERTICES));
        byte[] id = HBaseUtils.getFirstKeyFromMap(map);
        return id;
    }

    public byte[] getInVertexId() {
        Get get = new Get(id);
        HTable edgeTable = hGraph.getEdgeTable();
        Result result = null;
        try {
            result = edgeTable.get(get);
        } catch (IOException e) {
            LOG.info("Error while fetching out vertex");
            e.printStackTrace();
            return null;
        }

        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes(INVERTICES));
        byte[] id = HBaseUtils.getFirstKeyFromMap(map);
        return id;
    }


}
