package com.davydmaker.metabase.jdbc;

import com.davydmaker.metabase.api.MetabaseColumnInfo;
import com.davydmaker.metabase.api.MetabaseQueryResult;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * JDBC ResultSet implementation for Metabase driver.
 * 
 * Represents query results returned from Metabase API with full JDBC compliance
 * for read-only operations and forward-only navigation.
 */
public class MetabaseResultSet implements ResultSet {
    private final Statement statement;
    private final MetabaseQueryResult queryResult;
    private boolean closed = false;
    private boolean wasNull = false;
    
    /**
     * Creates a new Metabase result set.
     * 
     * @param statement Statement that generated this ResultSet
     * @param queryResult Query result from Metabase API
     */
    public MetabaseResultSet(Statement statement, MetabaseQueryResult queryResult) {
        this.statement = statement;
        this.queryResult = queryResult;
    }
    
    @Override
    public boolean next() throws SQLException {
        boolean hasNext = queryResult.next();
        return hasNext;
    }
    
    @Override
    public void close() throws SQLException {
        if (!closed) {
            closed = true;
        }
    }
    
    @Override
    public boolean wasNull() throws SQLException {        
        return wasNull;
    }
    
    @Override
    public String getString(int columnIndex) throws SQLException {        
        Object value = queryResult.getValue(columnIndex);
        wasNull = (value == null);
        return value != null ? value.toString() : null;
    }
    
    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {        
        Object value = queryResult.getValue(columnIndex);
        wasNull = (value == null);
        
        if (value == null) return false;
        if (value instanceof Boolean) return (Boolean) value;
        if (value instanceof Number) return ((Number) value).intValue() != 0;
        
        String str = value.toString().toLowerCase();
        return "true".equals(str) || "1".equals(str) || "yes".equals(str);
    }
    
    @Override
    public byte getByte(int columnIndex) throws SQLException {        
        Object value = queryResult.getValue(columnIndex);
        wasNull = (value == null);
        
        if (value == null) return 0;
        if (value instanceof Number) return ((Number) value).byteValue();
        
        try {
            return Byte.parseByte(value.toString());
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert to byte: " + value);
        }
    }
    
    @Override
    public short getShort(int columnIndex) throws SQLException {        
        Object value = queryResult.getValue(columnIndex);
        wasNull = (value == null);
        
        if (value == null) return 0;
        if (value instanceof Number) return ((Number) value).shortValue();
        
        try {
            return Short.parseShort(value.toString());
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert to short: " + value);
        }
    }
    
    @Override
    public int getInt(int columnIndex) throws SQLException {        
        Object value = queryResult.getValue(columnIndex);
        wasNull = (value == null);
        
        if (value == null) return 0;
        if (value instanceof Number) return ((Number) value).intValue();
        
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert to int: " + value);
        }
    }
    
    @Override
    public long getLong(int columnIndex) throws SQLException {        
        Object value = queryResult.getValue(columnIndex);
        wasNull = (value == null);
        
        if (value == null) return 0L;
        if (value instanceof Number) return ((Number) value).longValue();
        
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert to long: " + value);
        }
    }
    
    @Override
    public float getFloat(int columnIndex) throws SQLException {        
        Object value = queryResult.getValue(columnIndex);
        wasNull = (value == null);
        
        if (value == null) return 0.0f;
        if (value instanceof Number) return ((Number) value).floatValue();
        
        try {
            return Float.parseFloat(value.toString());
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert to float: " + value);
        }
    }
    
    @Override
    public double getDouble(int columnIndex) throws SQLException {        
        Object value = queryResult.getValue(columnIndex);
        wasNull = (value == null);
        
        if (value == null) return 0.0;
        if (value instanceof Number) return ((Number) value).doubleValue();
        
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert to double: " + value);
        }
    }
    
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {        
        Object value = queryResult.getValue(columnIndex);
        wasNull = (value == null);
        
        if (value == null) return null;
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).setScale(scale, BigDecimal.ROUND_HALF_UP);
        }
        
        try {
            return new BigDecimal(value.toString()).setScale(scale, BigDecimal.ROUND_HALF_UP);
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert to BigDecimal: " + value);
        }
    }
    
    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {        
        Object value = queryResult.getValue(columnIndex);
        wasNull = (value == null);
        
        if (value == null) return null;
        if (value instanceof byte[]) return (byte[]) value;
        
        return value.toString().getBytes();
    }
    
    @Override
    public Date getDate(int columnIndex) throws SQLException {        
        Object value = queryResult.getValue(columnIndex);
        wasNull = (value == null);
        
        if (value == null) return null;
        if (value instanceof Date) return (Date) value;
        if (value instanceof java.util.Date) return new Date(((java.util.Date) value).getTime());
        
        try {
            return Date.valueOf(value.toString());
        } catch (IllegalArgumentException e) {
            throw new SQLException("Cannot convert to Date: " + value);
        }
    }
    
    @Override
    public Time getTime(int columnIndex) throws SQLException {        
        Object value = queryResult.getValue(columnIndex);
        wasNull = (value == null);
        
        if (value == null) return null;
        if (value instanceof Time) return (Time) value;
        if (value instanceof java.util.Date) return new Time(((java.util.Date) value).getTime());
        
        try {
            return Time.valueOf(value.toString());
        } catch (IllegalArgumentException e) {
            throw new SQLException("Cannot convert to Time: " + value);
        }
    }
    
    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {        
        Object value = queryResult.getValue(columnIndex);
        wasNull = (value == null);
        
        if (value == null) return null;
        if (value instanceof Timestamp) return (Timestamp) value;
        if (value instanceof java.util.Date) return new Timestamp(((java.util.Date) value).getTime());
        
        try {
            return Timestamp.valueOf(value.toString());
        } catch (IllegalArgumentException e) {
            throw new SQLException("Cannot convert to Timestamp: " + value);
        }
    }
    
    @Override
    public Object getObject(int columnIndex) throws SQLException {        
        Object value = queryResult.getValue(columnIndex);
        wasNull = (value == null);
        return value;
    }
    
    @Override
    public String getString(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getString(columnIndex);
    }
    
    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBoolean(columnIndex);
    }
    
    @Override
    public byte getByte(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getByte(columnIndex);
    }
    
    @Override
    public short getShort(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getShort(columnIndex);
    }
    
    @Override
    public int getInt(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getInt(columnIndex);
    }
    
    @Override
    public long getLong(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getLong(columnIndex);
    }
    
    @Override
    public float getFloat(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getFloat(columnIndex);
    }
    
    @Override
    public double getDouble(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getDouble(columnIndex);
    }
    
    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBigDecimal(columnIndex, scale);
    }
    
    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBytes(columnIndex);
    }
    
    @Override
    public Date getDate(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getDate(columnIndex);
    }
    
    @Override
    public Time getTime(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getTime(columnIndex);
    }
    
    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getTimestamp(columnIndex);
    }
    
    @Override
    public Object getObject(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getObject(columnIndex);
    }
    
    @Override
    public boolean isBeforeFirst() throws SQLException {        
        return queryResult.isBeforeFirst();
    }
    
    @Override
    public boolean isAfterLast() throws SQLException {        
        return queryResult.isAfterLast();
    }
    
    @Override
    public boolean isFirst() throws SQLException {        
        return queryResult.isFirst();
    }
    
    @Override
    public boolean isLast() throws SQLException {        
        return queryResult.isLast();
    }
    
    @Override
    public void beforeFirst() throws SQLException {
        queryResult.beforeFirst();
    }
    
    @Override
    public void afterLast() throws SQLException {        
        queryResult.afterLast();
    }
    
    @Override
    public boolean first() throws SQLException {
        return queryResult.first();
    }
    
    @Override
    public boolean last() throws SQLException {
        return queryResult.last();
    }
    
    @Override
    public int getRow() throws SQLException {
        return queryResult.getRow();
    }
    
    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException("Absolute positioning not supported");
    }
    
    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("Relative positioning not supported");
    }
    
    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException("Reverse navigation not supported");
    }
    
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {        
        return new MetabaseResultSetMetaData(queryResult.getColumns());
    }
    
    @Override
    public int findColumn(String columnLabel) throws SQLException {        
        int columnIndex = queryResult.getColumnIndex(columnLabel);
        if (columnIndex == -1) {
            throw new SQLException("Column not found: " + columnLabel);
        }
        return columnIndex;
    }
    
    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("ASCII streams not supported");
    }
    
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Unicode streams not supported");
    }
    
    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary streams not supported");
    }
    
    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("ASCII streams not supported");
    }
    
    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Unicode streams not supported");
    }
    
    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary streams not supported");
    }
    
    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }
    
    @Override
    public void clearWarnings() {
    }
    
    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("Named cursors not supported");
    }
    
    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams not supported");
    }
    
    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams not supported");
    }
    
    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Object value = queryResult.getValue(columnIndex);
        wasNull = (value == null);
        
        if (value == null) return null;
        if (value instanceof BigDecimal) return (BigDecimal) value;
        
        try {
            return new BigDecimal(value.toString());
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert to BigDecimal: " + value);
        }
    }
    
    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBigDecimal(columnIndex);
    }
    
    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }
    
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != FETCH_FORWARD) {
            throw new SQLException("Only FETCH_FORWARD is supported");
        }
    }
    
    @Override
    public int getFetchDirection() throws SQLException {
        return FETCH_FORWARD;
    }
    
    @Override
    public void setFetchSize(int rows) throws SQLException {
    }
    
    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }
    
    @Override
    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }
    
    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }
    
    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }
    
    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }
    
    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }
    
    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }
    
    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }
    
    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref not supported");
    }
    
    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }
    
    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }
    
    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array not supported");
    }
    
    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnLabel);
    }
    
    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref not supported");
    }
    
    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }
    
    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }
    
    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array not supported");
    }
    
    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }
    
    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(columnLabel);
    }
    
    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(columnIndex);
    }
    
    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(columnLabel);
    }
    
    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }
    
    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(columnLabel);
    }
    
    @Override
    public URL getURL(int columnIndex) throws SQLException {
        Object value = queryResult.getValue(columnIndex);
        wasNull = (value == null);
        
        if (value == null) return null;
        
        try {
            return new URL(value.toString());
        } catch (Exception e) {
            throw new SQLException("Cannot convert to URL: " + value);
        }
    }
    
    @Override
    public URL getURL(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getURL(columnIndex);
    }
    
    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId not supported");
    }
    
    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId not supported");
    }
    
    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public int getHoldability() throws SQLException {
        return CLOSE_CURSORS_AT_COMMIT;
    }
    
    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }
    
    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }
    
    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }
    
    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }
    
    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }
    
    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }
    
    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacter streams not supported");
    }
    
    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacter streams not supported");
    }
    
    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }
    
    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        Object value = queryResult.getValue(columnIndex);
        wasNull = (value == null);
        
        if (value == null) return null;
        
        if (type.isInstance(value)) {
            return type.cast(value);
        }
        
        throw new SQLException("Cannot convert to " + type.getName() + ": " + value);
    }
    
    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getObject(columnIndex, type);
    }
    
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }
    
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
    
    /**
     * Checks if result set is closed and throws exception if so.
     * 
     * @throws SQLException If result set is closed
     */
    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed");
        }
    }
} 