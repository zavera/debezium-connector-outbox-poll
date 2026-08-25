package io.debezium.connector.outboxpoll;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.NoSuchElementException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Streams the watched table ordered by its id column, one row at a time, so
 * the sort-merge diff can consume it without materialising the whole scan in
 * memory. Each row is serialised to canonical JSON (column order fixed by
 * the query's result set metadata) and checksummed.
 */
public final class TableScanner implements Iterator<RowScan>, AutoCloseable {

    private final PreparedStatement statement;
    private final ResultSet resultSet;
    private final String idColumn;
    private final ObjectMapper mapper = new ObjectMapper();
    private Boolean hasNextCache;

    public TableScanner(Connection connection, String tableName, String idColumn, int fetchSize) throws SQLException {
        this.idColumn = idColumn;
        String sql = "SELECT * FROM " + tableName + " ORDER BY " + idColumn + " ASC";
        this.statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        this.statement.setFetchSize(fetchSize);
        this.resultSet = statement.executeQuery();
    }

    @Override
    public boolean hasNext() {
        if (hasNextCache == null) {
            try {
                hasNextCache = resultSet.next();
            }
            catch (SQLException e) {
                throw new IllegalStateException("Failed to advance table scan", e);
            }
        }
        return hasNextCache;
    }

    @Override
    public RowScan next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        hasNextCache = null;
        try {
            return readCurrentRow();
        }
        catch (SQLException e) {
            throw new IllegalStateException("Failed to read row during table scan", e);
        }
    }

    private RowScan readCurrentRow() throws SQLException {
        ResultSetMetaData meta = resultSet.getMetaData();
        int columnCount = meta.getColumnCount();
        var row = new LinkedHashMap<String, Object>(columnCount);
        for (int col = 1; col <= columnCount; col++) {
            row.put(meta.getColumnLabel(col), resultSet.getObject(col));
        }
        long id = resultSet.getLong(idColumn);
        String json;
        try {
            json = mapper.writeValueAsString(row);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize row id=" + id, e);
        }
        return new RowScan(id, ChecksumUtil.checksum(json), json);
    }

    @Override
    public void close() throws SQLException {
        resultSet.close();
        statement.close();
    }
}
