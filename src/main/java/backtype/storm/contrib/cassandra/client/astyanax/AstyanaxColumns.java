package backtype.storm.contrib.cassandra.client.astyanax;

import java.util.Iterator;

import backtype.storm.contrib.cassandra.bolt.mapper.Columns;

import com.netflix.astyanax.model.ColumnList;

public class AstyanaxColumns<T> implements Columns<T> {
    private ColumnList<T> columns;
    
    public AstyanaxColumns(ColumnList<T> columns){
        this.columns = columns;
    }

    @Override
    public String getColumnValue(T columnName) {
        return columns.getColumnByName(columnName).getStringValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<T> getColumnNames() {
        return (Iterator<T>) this.columns.iterator();
    }    
}
