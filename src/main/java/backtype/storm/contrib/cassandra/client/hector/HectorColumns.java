package backtype.storm.contrib.cassandra.client.hector;

import java.util.Iterator;

import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import backtype.storm.contrib.cassandra.bolt.mapper.Columns;

public class HectorColumns<T> implements Columns<T> {

    private ColumnFamilyResult<String, T> columns; 
    
    public HectorColumns(ColumnFamilyResult<String, T> columns){
        this.columns = columns;
    }
    
    @Override
    public String getColumnValue(T columnName) {
        return columns.getString(columnName);
    }

    @Override
    public Iterator<T> getColumnNames() {
        return (Iterator<T>) columns.getColumnNames().iterator();
    }

}
