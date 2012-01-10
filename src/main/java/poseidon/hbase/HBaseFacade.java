package poseidon.hbase;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import poseidon.HEdge;
import poseidon.HVertex;
import poseidon.util.LogUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.logging.Logger;

import static poseidon.Constants.*;

/**
 * @author Ashwin
 */
public class HBaseFacade {


    private static Logger LOG = LogUtil.getLoggerFor(HBaseFacade.class);

    private HBaseAdmin hBaseAdmin;

    private final String graphName;


    public HTable getVertexTable() {
        return vertexTable;
    }

    public HTable getEdgeTable() {
        return edgeTable;
    }

    public HTable getVertexIndexTable() {
        return vertexIndexTable;
    }

    public HTable getEdgeIndexTable() {
        return edgeIndexTable;
    }

    private HTable vertexTable;
    private HTable edgeTable;
    private HTable vertexIndexTable;
    private HTable edgeIndexTable;


    public HBaseFacade(final String zookeeperQuorum, final String port, final String graphName, boolean createNew) {
        this.graphName = graphName;
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(ZOOKEEPER_QUORUM, zookeeperQuorum);
        configuration.set(ZOOKEEPER_PROPERTY_CLIENTPORT, port);

        try {
            hBaseAdmin = new HBaseAdmin(configuration);
            createTables(createNew);
        } catch (MasterNotRunningException e) {
            LOG.info("Hbase master is not running");
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            LOG.info("Zookeeper master is not running");
            e.printStackTrace();
        } catch (IOException e) {
            LOG.info("Could not create tables");
        }
    }


    private void createTables(boolean createNew) throws IOException {
        String vertexTableName = new StringBuilder().append(graphName).append(SEPERATOR).append(VERTEX).toString();
        String edgeTableName = new StringBuilder().append(graphName).append(SEPERATOR).append(EDGE).toString();
        String vertexIndexTableName = new StringBuilder().append(graphName).append(SEPERATOR).append(VERTEXINDEX).toString();
        String edgeIndexTableName = new StringBuilder().append(graphName).append(SEPERATOR).append(EDGEINDEX).toString();
        if (createNew) {
            LOG.info(String.format("Create table %s", vertexTableName));
            createVertexTable(vertexTableName);
            LOG.info(String.format("Create table %s", edgeTableName));
            createEdgeTable(edgeTableName);
            LOG.info(String.format("Create table %s and %s", vertexIndexTableName, edgeIndexTableName));
            createIndexTable(vertexIndexTableName);
            createIndexTable(edgeIndexTableName);
        }
        this.vertexTable = new HTable(hBaseAdmin.getConfiguration(), vertexTableName);
        this.edgeTable = new HTable(hBaseAdmin.getConfiguration(), edgeTableName);
        this.vertexIndexTable = new HTable(hBaseAdmin.getConfiguration(), vertexIndexTableName);
        this.edgeIndexTable = new HTable(hBaseAdmin.getConfiguration(), edgeIndexTableName);

    }


    private void createIndexTable(String indexTableName) throws IOException {
        if (!hBaseAdmin.tableExists(indexTableName)) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(indexTableName);
            HColumnDescriptor propertiesColumnDescriptor = new HColumnDescriptor(VALUES);
            tableDescriptor.addFamily(propertiesColumnDescriptor);
            hBaseAdmin.createTable(tableDescriptor);
        }

    }

    private void createEdgeTable(String edgeTableName) throws IOException {
        if (!hBaseAdmin.tableExists(edgeTableName)) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(edgeTableName);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(new StringBuilder().append(EDGE).append(PROPERTIES).toString());
            HColumnDescriptor inVertexColumnDescriptor = new HColumnDescriptor(INVERTICES);
            HColumnDescriptor outVertexColumnDescriptor = new HColumnDescriptor(OUTVERTICES);
            tableDescriptor.addFamily(columnDescriptor);
            tableDescriptor.addFamily(inVertexColumnDescriptor);
            tableDescriptor.addFamily(outVertexColumnDescriptor);
            hBaseAdmin.createTable(tableDescriptor);
        }

    }

    private void createVertexTable(String vertexTableName) throws IOException {
        if (!hBaseAdmin.tableExists(vertexTableName)) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(vertexTableName);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(new StringBuilder().append(VERTEX).append(PROPERTIES).toString());
            HColumnDescriptor inEdgesColumnDescriptor = new HColumnDescriptor(INEDGES);
            HColumnDescriptor outEdgesColumnDescriptor = new HColumnDescriptor(OUTEDGES);
            tableDescriptor.addFamily(columnDescriptor);
            tableDescriptor.addFamily(inEdgesColumnDescriptor);
            tableDescriptor.addFamily(outEdgesColumnDescriptor);
            hBaseAdmin.createTable(tableDescriptor);
        }
    }

    public void putInVertexTable(Put put) {
        try {
            vertexTable.put(put);
        } catch (IOException e) {
            LOG.info("Exception while adding vertex");
            e.printStackTrace();
        }
    }

    public Result getFromVertexTable(Get get) {
        Result result = null;
        try {
            result = vertexTable.get(get);
        } catch (IOException e) {
            LOG.info("Exception while getting vertex");
            e.printStackTrace();
        }
        return result;
    }

    public void putInEdgeTable(Put put) {
        try {
            edgeTable.put(put);
        } catch (IOException e) {
            LOG.info("Exception while putting edge");
            e.printStackTrace();
        }
    }

    public void clearAll() {
        try {
            disableTables();
            deleteTables();
            this.createTables(true);
            enableTables();

        } catch (IOException e) {
            LOG.info("Exception while flushing tables");
            e.printStackTrace();
        } catch (Exception e) {
            LOG.info("Exception while flushing tables");
            e.printStackTrace();
        }
    }

    private void enableTables() throws IOException {
        if (hBaseAdmin.isTableDisabled(graphName + SEPERATOR + VERTEX))
            hBaseAdmin.enableTable(graphName + SEPERATOR + VERTEX);
        if (hBaseAdmin.isTableDisabled(graphName + SEPERATOR + EDGE))
            hBaseAdmin.enableTable(graphName + SEPERATOR + EDGE);
        if (hBaseAdmin.isTableDisabled(graphName + SEPERATOR + VERTEXINDEX))
            hBaseAdmin.enableTable(graphName + SEPERATOR + VERTEXINDEX);
        if (hBaseAdmin.isTableDisabled(graphName + SEPERATOR + EDGEINDEX))
            hBaseAdmin.enableTable(graphName + SEPERATOR + EDGEINDEX);
    }

    private void deleteTables() throws IOException {
        hBaseAdmin.deleteTable(graphName + SEPERATOR + VERTEX);
        hBaseAdmin.deleteTable(graphName + SEPERATOR + EDGE);
        hBaseAdmin.deleteTable(graphName + SEPERATOR + VERTEXINDEX);
        hBaseAdmin.deleteTable(graphName + SEPERATOR + EDGEINDEX);
    }

    private void disableTables() throws IOException {
        hBaseAdmin.disableTable(graphName + SEPERATOR + VERTEX);
        hBaseAdmin.disableTable(graphName + SEPERATOR + EDGE);
        hBaseAdmin.disableTable(graphName + SEPERATOR + VERTEXINDEX);
        hBaseAdmin.disableTable(graphName + SEPERATOR + EDGEINDEX);
    }


    public void shutDown() {
        this.clearAll();

    }

    public HTable getIndexTable(Class indexClass) {
        if (indexClass.equals(HVertex.class) || indexClass.equals(Vertex.class))
            return this.getVertexIndexTable();
        if (indexClass.equals(HEdge.class) || indexClass.equals(Edge.class))
            return this.getEdgeIndexTable();

        return null;


    }

    public void startUp() {
        try {
            enableTables();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
