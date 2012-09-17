package backtype.storm.contrib.cassandra.client.hector;

import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import backtype.storm.contrib.cassandra.bolt.mapper.Columns;
import backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper;
import backtype.storm.contrib.cassandra.client.CassandraClient;
import backtype.storm.tuple.Tuple;

public class HectorClient<T> extends CassandraClient<T> {
    private Cluster cluster;
    private Keyspace keyspace;

    @Override
    public void start(String cassandraHost, String cassandraKeyspace) {
        CassandraHostConfigurator chc = new CassandraHostConfigurator(cassandraHost);
        chc.setAutoDiscoverHosts(true);
        chc.setRunAutoDiscoveryAtStartup(false);
        this.cluster = HFactory.getOrCreateCluster("cirrus-cluster", chc);
        this.keyspace = HFactory.createKeyspace(cassandraKeyspace, this.cluster);

    }

    @Override
    public void stop() {
        this.cluster.getConnectionManager().shutdown();
    }

    @Override
    public Columns<T> lookup(String columnFamilyName, String rowKey) throws Exception {
        ColumnFamilyTemplate<String, T> template = new ThriftColumnFamilyTemplate<String, T>(this.keyspace,
                columnFamilyName, new StringSerializer(), this.getColumnNameSerializer());
        ColumnFamilyResult<String, T> result = template.queryColumns(rowKey);
        return new HectorColumns<T>(result);
    }

    @Override
    public void writeTuple(Tuple input, TupleMapper<T> tupleMapper) throws Exception {
        String rowKey = (String) tupleMapper.mapToRowKey(input);
        Mutator<String> mutator = HFactory.createMutator(this.keyspace, new StringSerializer());
        addTupleToMutation(input, rowKey, mutator, tupleMapper);
        mutator.execute();
    }

    private void addTupleToMutation(Tuple input, String rowKey, Mutator<String> mutator, TupleMapper<T> tupleMapper) {
        Map<T, String> columns = tupleMapper.mapToColumns(input);
        for (Map.Entry<T, String> entry : columns.entrySet()) {
            mutator.addInsertion(rowKey, tupleMapper.mapToColumnFamily(input),
                    HFactory.createColumn(entry.getKey(), entry.getValue()));
        }
    }

    @Override
    public void writeTuples(List<Tuple> inputs, TupleMapper<T> tupleMapper) throws Exception {
        Mutator<String> mutator = HFactory.createMutator(this.keyspace, new StringSerializer());
        for(Tuple input : inputs){
            String rowKey = (String) tupleMapper.mapToRowKey(input);
            this.addTupleToMutation(input, rowKey, mutator, tupleMapper);
        }
        mutator.execute();

    }

    @SuppressWarnings("unchecked")
    private Serializer<T> getColumnNameSerializer() {
        if (this.getColumnNameClass().equals(String.class)) {
            return (Serializer<T>) StringSerializer.get();
        } else {
            return (Serializer<T>) CompositeSerializer.get();
        }
    }
}
