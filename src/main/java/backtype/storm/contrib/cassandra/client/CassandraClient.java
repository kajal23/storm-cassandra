package backtype.storm.contrib.cassandra.client;

import java.util.List;

import backtype.storm.contrib.cassandra.bolt.mapper.Columns;
import backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper;
import backtype.storm.tuple.Tuple;

public abstract class CassandraClient<T> {

    private Class<T> columnNameClass;

    public void setColumnNameClass(Class<T> columnNameClass) {
        this.columnNameClass = columnNameClass;
    }

    protected Class<T> getColumnNameClass(){
        return this.columnNameClass;
    }

    public abstract void start(String cassandraHost, String cassandraKeyspace);

    public abstract void stop();

    public abstract Columns<T> lookup(String columnFamilyName, String rowKey) throws Exception;

    public abstract void writeTuple(Tuple input, TupleMapper<T> tupleMapper) throws Exception;

    public abstract void writeTuples(List<Tuple> inputs, TupleMapper<T> tupleMapper) throws Exception;

}