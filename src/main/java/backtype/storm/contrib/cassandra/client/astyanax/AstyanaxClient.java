package backtype.storm.contrib.cassandra.client.astyanax;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.cassandra.bolt.mapper.Columns;
import backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper;
import backtype.storm.contrib.cassandra.client.CassandraClient;
import backtype.storm.tuple.Tuple;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 *
 * @author tgoetz
 *
 */
public class AstyanaxClient<T> extends CassandraClient<T> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(AstyanaxClient.class);
    private AstyanaxContext<Keyspace> astyanaxContext;
    protected Cluster cluster;
    protected Keyspace keyspace;

    /*
     * (non-Javadoc)
     *
     * @see
     * backtype.storm.contrib.cassandra.client.CassandraClient#start(java.lang
     * .String, java.lang.String)
     */
    @Override
    public void start(String cassandraHost, String cassandraKeyspace) {
        try {
            this.astyanaxContext = new AstyanaxContext.Builder()
                    .forCluster("ClusterName")
                    .forKeyspace(cassandraKeyspace)
                    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
                    .withConnectionPoolConfiguration(
                            new ConnectionPoolConfigurationImpl("MyConnectionPool").setMaxConnsPerHost(1).setSeeds(
                                    cassandraHost)).withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                    .buildKeyspace(ThriftFamilyFactory.getInstance());

            this.astyanaxContext.start();
            this.keyspace = this.astyanaxContext.getEntity();
            // test the connection
            LOG.info("Connecting to [" + cassandraHost + "]");
            this.keyspace.describeKeyspace();
        } catch (Throwable e) {
            LOG.warn("Astyanax initialization failed.", e);
            throw new IllegalStateException("Failed to prepare Astyanax", e);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see backtype.storm.contrib.cassandra.client.CassandraClient#stop()
     */
    @Override
    public void stop() {
        this.astyanaxContext.shutdown();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * backtype.storm.contrib.cassandra.client.CassandraClient#lookup(java.lang
     * .String, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public Columns<T> lookup(String columnFamilyName, String rowKey) throws Exception {
        ColumnFamily<String, T> columnFamily = new ColumnFamily<String, T>(columnFamilyName,
                StringSerializer.get(), getColumnNameSerializer());
        OperationResult<ColumnList<T>> result;
        result = this.keyspace.prepareQuery(columnFamily).getKey(rowKey).execute();
        ColumnList<T> columns = (ColumnList<T>) result.getResult();
        return new AstyanaxColumns<T>(columns);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * backtype.storm.contrib.cassandra.client.CassandraClient#writeTuple(backtype
     * .storm.tuple.Tuple,
     * backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper)
     */
    @Override
    public void writeTuple(Tuple input, TupleMapper<T> tupleMapper) throws Exception {
        String columnFamilyName = tupleMapper.mapToColumnFamily(input);
        String rowKey = (String) tupleMapper.mapToRowKey(input);
        MutationBatch mutation = keyspace.prepareMutationBatch();
        ColumnFamily<String, T> columnFamily = new ColumnFamily<String, T>(columnFamilyName, StringSerializer.get(),
                this.getColumnNameSerializer(tupleMapper));
        this.addTupleToMutation(input, columnFamily, rowKey, mutation, tupleMapper);
        mutation.execute();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * backtype.storm.contrib.cassandra.client.CassandraClient#writeTuples(java
     * .util.List, backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper)
     */
    @Override
    public void writeTuples(List<Tuple> inputs, TupleMapper<T> tupleMapper) throws Exception {
        MutationBatch mutation = keyspace.prepareMutationBatch();
        for (Tuple input : inputs) {
            String columnFamilyName = tupleMapper.mapToColumnFamily(input);
            String rowKey = (String) tupleMapper.mapToRowKey(input);
            ColumnFamily<String, T> columnFamily = new ColumnFamily<String, T>(columnFamilyName,
                    StringSerializer.get(), this.getColumnNameSerializer(tupleMapper));
            this.addTupleToMutation(input, columnFamily, rowKey, mutation, tupleMapper);
        }
        try {
            mutation.execute();
        } catch (Exception e) {
            LOG.error("Could not execute mutation.", e);
        }
    }

    private void addTupleToMutation(Tuple input, ColumnFamily<String, T> columnFamily, String rowKey,
            MutationBatch mutation, TupleMapper<T> tupleMapper) {
        Map<T, String> columns = tupleMapper.mapToColumns(input);
        for (Map.Entry<T, String> entry : columns.entrySet()) {
            mutation.withRow(columnFamily, rowKey).putColumn(entry.getKey(), entry.getValue(), null);
        }
    }

    @SuppressWarnings("unchecked")
    private Serializer<T> getColumnNameSerializer(TupleMapper<T> tupleMapper) {
        if (this.getColumnNameClass().equals(String.class)) {
            return (Serializer<T>) StringSerializer.get();
        } else {
            // TODO: Cache this instance.
            return new AnnotatedCompositeSerializer<T>(this.getColumnNameClass());
        }
    }

    @SuppressWarnings("unchecked")
    private Serializer<T> getColumnNameSerializer() {
        if (this.getColumnNameClass().equals(String.class)) {
            return (Serializer<T>) StringSerializer.get();
        } else {
            return new AnnotatedCompositeSerializer<T>(this.getColumnNameClass());
        }
    }
}
