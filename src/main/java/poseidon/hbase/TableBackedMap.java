package poseidon.hbase;

import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Vertex;
import poseidon.HEdge;
import poseidon.HGraph;
import poseidon.serialize.ByteArraySerializer;
import poseidon.util.LogUtil;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import poseidon.HVertex;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

import static poseidon.Constants.VALUES;

/**
 * @author Ashwin
 */
public class TableBackedMap implements Map<Object, Set<Element>> {

    private HTable backingTable;
    private HGraph hGraph;

    private Class<Element> indexClass;

    public String backingTableName() {
        return new String(this.backingTable.getTableName());
    }


    private static Logger LOG = LogUtil.getLoggerFor(TableBackedMap.class);

    private TableBackedMap(HTable backingTable, HGraph hGraph, Class<Element> indexClass) {
        this.indexClass = indexClass;
        this.backingTable = backingTable;
        this.hGraph = hGraph;
    }

    private TableBackedMap() {

    }


    @Override
    public int size() {
        throw new NotImplementedException();
    }

    @Override
    public boolean isEmpty() {
        throw new NotImplementedException();
    }

    @Override
    public boolean containsKey(Object key) {
        throw new NotImplementedException();
    }

    @Override
    public boolean containsValue(Object value) {
        throw new NotImplementedException();
    }

    @Override
    public Set<Element> get(Object key) {
        Get get = new Get(ByteArraySerializer.fromObject(key));
        Result result;
        try {
            result = backingTable.get(get);
        } catch (IOException e) {
            LOG.severe("Cannot get from backing table");
            e.printStackTrace();
            return null;
        }

        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes(VALUES));
        if (null == map)
            return null;

        HashSet<Element> elementHashSet = new HashSet<Element>();

        for (byte[] byteArray : map.keySet()) {

            if (indexClass.equals(HVertex.class) || indexClass.equals(Vertex.class)) {
                HVertex hVertex = new HVertex(hGraph);
                hVertex.setId(byteArray);
                elementHashSet.add(hVertex);
            } else {
                final HEdge hEdge = new HEdge(hGraph);
                hEdge.setId(byteArray);
                elementHashSet.add(hEdge);
            }
        }

        return elementHashSet;

    }

    @Override
    public Set<Element> put(Object key, Set<Element> value) {
         Put put = new Put(ByteArraySerializer.fromObject(key));
        for (Element val : value)
            put.add(Bytes.toBytes(VALUES), (byte[]) val.getId(), (byte[]) val.getId());
        try {
            backingTable.put(put);
        } catch (IOException e) {
            LOG.severe("Cannot put into backing table");
            e.printStackTrace();
            return null;
        }
        return value;
    }


    public void removeSingleElement(Object key, Element value) {
        final Delete delete = new Delete(ByteArraySerializer.fromObject(key));
        delete.deleteColumns(Bytes.toBytes(VALUES), (byte[]) value.getId());
        try {
            backingTable.delete(delete);
        } catch (IOException e) {
            LOG.severe("Cannot delete from backing table");
            e.printStackTrace();
        }
    }

    @Override
    public Set<Element> remove(Object key) {
        Delete del = new Delete(ByteArraySerializer.fromObject(key));
        try {
            backingTable.delete(del);
        } catch (IOException e) {
            LOG.severe("Error while deleting from table");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void putAll(Map<? extends Object, ? extends Set<Element>> m) {
        throw new NotImplementedException();
    }

    @Override
    public void clear() {
        throw new NotImplementedException();
    }

    @Override
    public Set<Object> keySet() {
        throw new NotImplementedException();
    }

    @Override
    public Collection<Set<Element>> values() {
        throw new NotImplementedException();
    }

    @Override
    public Set<Entry<Object, Set<Element>>> entrySet() {
        throw new NotImplementedException();
    }

    public static TableBackedMap buildFrom(HTable backingTable, HGraph hGraph, Class<Element> indexClass) {
        return new TableBackedMap(backingTable, hGraph, indexClass);
    }

}
