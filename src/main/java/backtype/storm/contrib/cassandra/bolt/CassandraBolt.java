package backtype.storm.contrib.cassandra.bolt;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper;
import backtype.storm.contrib.cassandra.client.CassandraClient;
import backtype.storm.contrib.cassandra.client.ClientPool;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class CassandraBolt<T> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraBolt.class);
    public static String CASSANDRA_HOST = "cassandra.host";
    public static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
    public static final String CASSANDRA_BATCH_MAX_SIZE = "cassandra.batch.max_size";
    public static String CASSANDRA_CLIENT_CLASS = "cassandra.client.class";

    private String cassandraHost;
    private String cassandraKeyspace;
    private Class columnNameClass;
    private String clientClass;
    protected TupleMapper<T> tupleMapper;

    // protected AstyanaxContext<Keyspace> astyanaxContext;

    public CassandraBolt(TupleMapper<T> tupleMapper, Class columnNameClass) {
        this.tupleMapper = tupleMapper;
        this.columnNameClass = columnNameClass;
        LOG.debug("Creating Cassandra Bolt (" + this + ")");
    }

    public CassandraBolt(TupleMapper<T> tupleMapper) {
        this(tupleMapper, String.class);
    }

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
        this.cassandraHost = (String) stormConf.get(CASSANDRA_HOST);
        this.cassandraKeyspace = (String) stormConf.get(CASSANDRA_KEYSPACE);
        this.clientClass = (String) stormConf.get(CASSANDRA_CLIENT_CLASS);
    }

    public CassandraClient<T> getClient(){
        return ClientPool.getClient(this.cassandraHost, this.cassandraKeyspace, this.columnNameClass, this.clientClass);
    }

    public void cleanup() {
        // No longer stop the client since it might be shared.
        // TODO: Come back and fix this.
        // getClient().stop();
    }

    public void writeTuple(Tuple input) throws Exception {
        getClient().writeTuple(input, this.tupleMapper);
    }

    public void writeTuples(List<Tuple> inputs) throws Exception {
        getClient().writeTuples(inputs, this.tupleMapper);
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
