package poseidon;

import com.tinkerpop.blueprints.pgm.AutomaticIndex;
import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.impls.WrappingCloseableSequence;
import poseidon.hbase.HBaseFacade;
import poseidon.hbase.TableBackedMap;
import poseidon.util.LogUtil;
import org.apache.hadoop.hbase.client.HTable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;


/**
 * @author Ashwin
 */
public class HIndex<T extends Element> implements AutomaticIndex<T> {

    private Logger LOG = LogUtil.getLoggerFor(HIndex.class);

    private HGraph graph;

    private Map<String, TableBackedMap> underlying = new HashMap<String, TableBackedMap>();

    public HIndex(HGraph hGraph, String indexName, Class<T> indexClass, Set<String> indexKeys, HBaseFacade underlying) {
        this.graph = hGraph;
        this.indexName = indexName;
        this.indexClass = indexClass;
        hBase = underlying;
        if (indexKeys != null)
            this.indexKeys = new HashSet<String>(indexKeys);
        else {
            this.indexKeys = null;
        }
    }


    private final String indexName;
    private Class<T> indexClass;
    private HBaseFacade hBase;
    private Set<String> indexKeys;


    @Override
    public Set<String> getAutoIndexKeys() {
        return indexKeys;
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public Class<T> getIndexClass() {
        return indexClass;
    }

    @Override
    public Type getIndexType() {
        return Index.Type.AUTOMATIC;
    }

    @Override
    public void put(String key, Object value, T element) {
        if (this.indexClass.isAssignableFrom(element.getClass())) {
            TableBackedMap map = this.underlying.get(key);
            if (null == map) {
                HTable indexTable = this.hBase.getIndexTable(indexClass);
                if (null == indexTable) {
                    throw new RuntimeException("Index could not be created");
                }
                underlying.put(key, TableBackedMap.buildFrom(indexTable, graph, (Class<Element>) indexClass));
            }
        } else {
            LOG.info("Index classes do not match !!");
            return;
        }

        TableBackedMap map = this.underlying.get(key);
        Set<Element> elementSet = map.get(value);
        if (null == elementSet) {
            elementSet = new HashSet<Element>();
            elementSet.add(element);
            map.put(value, elementSet);
        } else {
            elementSet.add(element);
            map.put(value, elementSet);
        }

    }

    @Override
    public CloseableSequence<T> get(String key, Object value) {
        TableBackedMap map = underlying.get(key);
        if (null == map)
            return new WrappingCloseableSequence<T>(new HashSet<T>());
        Set<Element> set = map.get(value);
        if (null == set)
            return new WrappingCloseableSequence<T>(new HashSet<T>());
        else
            return new WrappingCloseableSequence<T>((Iterable<T>) set);
    }

    @Override
    public long count(String key, Object value) {
        TableBackedMap map = this.underlying.get(key);
        if (null == map)
            return 0;
        Set<Element> elementSet = map.get(value);
        return elementSet.size();
    }

    @Override
    public void remove(String key, Object value, T element) {
        TableBackedMap map = this.underlying.get(key);
        if (null == map)
            return;
        Set<Element> elementSet = map.get(value);
        if (null == elementSet)
            return;
        if (elementSet.size() == 0)
            map.remove(value);
        map.removeSingleElement(value, element);

    }

    protected void autoUpdate(final String key, final Object newValue, final Object oldValue, final T element) {
        if (this.getIndexClass().isAssignableFrom(element.getClass()) && (this.indexKeys == null || this.indexKeys.contains(key))) {
            if (oldValue != null)
                this.remove(key, oldValue, element);
            this.put(key, newValue, element);
        }
    }

    protected void autoRemove(final String key, final Object oldValue, final T element) {
        if (this.getIndexClass().isAssignableFrom(element.getClass()) && (this.indexKeys == null || this.indexKeys.contains(key))) {
            this.remove(key, oldValue, element);
        }
    }


}
