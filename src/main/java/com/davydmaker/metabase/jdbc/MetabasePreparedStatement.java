package com.davydmaker.metabase.jdbc;

import com.davydmaker.metabase.api.MetabaseApiClient;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * PreparedStatement implementation for the Metabase JDBC driver.
 * 
 * This class extends MetabaseStatement and adds support for prepared
 * parameters in SQL queries.
 * 
 */
public class MetabasePreparedStatement extends MetabaseStatement implements PreparedStatement {
    private final String originalSql;
    private final Map<Integer, Object> parameters = new HashMap<>();
    
    /**
     * Creates a new MetabasePreparedStatement.
     * 
     * @param connection the Metabase connection
     * @param apiClient the Metabase API client
     * @param databaseName the database name
     * @param sql the SQL query with parameters
     */
    public MetabasePreparedStatement(MetabaseConnection connection, MetabaseApiClient apiClient, 
                                   String databaseName, String sql) {
        super(connection, apiClient, databaseName);
        this.originalSql = sql;
    }
    
    @Override
    public ResultSet executeQuery() throws SQLException {
        String sql = buildFinalSql();
        return super.executeQuery(sql);
    }
    
    @Override
    public int executeUpdate() throws SQLException {        
        String sql = buildFinalSql();
        return super.executeUpdate(sql);
    }
    
    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {        
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, null);
    }
    
    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {        
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {        
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {        
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {        
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {        
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {        
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {        
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {        
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setString(int parameterIndex, String x) throws SQLException {        
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {        
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {        
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {        
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {        
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ASCII streams not supported");
    }
    
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Unicode streams not supported");
    }
    
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary streams not supported");
    }
    
    @Override
    public void clearParameters() throws SQLException {
        parameters.clear();
    }
    
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {        
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {        
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public boolean execute() throws SQLException {        
        String sql = buildFinalSql();
        return super.execute(sql);
    }
    
    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch processing not supported");
    }
    
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams not supported");
    }
    
    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref not supported");
    }
    
    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }
    
    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }
    
    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array not supported");
    }
    
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        try {
            String metadataQuery = buildMetadataQuery();
            ResultSet rs = super.executeQuery(metadataQuery);
            return rs.getMetaData();
        } catch (Exception e) {
            
            return null;
        }
    }
    
    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setDate(parameterIndex, x);
    }
    
    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setTime(parameterIndex, x);
    }
    
    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(parameterIndex, x);
    }
    
    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }
    
    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        validateParameterIndex(parameterIndex);
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("ParameterMetaData not supported");
    }
    
    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId not supported");
    }
    
    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }
    
    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacter streams not supported");
    }
    
    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }
    
    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }
    
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }
    
    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }
    
    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }
    
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x, targetSqlType);
    }
    
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ASCII streams not supported");
    }
    
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary streams not supported");
    }
    
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams not supported");
    }
    
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ASCII streams not supported");
    }
    
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary streams not supported");
    }
    
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams not supported");
    }
    
    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacter streams not supported");
    }
    
    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }
    
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }
    
    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }
    
    /**
     * Validate if the parameter index is valid.
     */
    private void validateParameterIndex(int parameterIndex) throws SQLException {
        if (parameterIndex < 1) {
            throw new SQLException("Parameter index must be >= 1, received: " + parameterIndex);
        }
    }
    
    /**
     * Builds the final SQL string by replacing parameter placeholders.
     * 
     * @return the final SQL string with parameters replaced
     * @throws SQLException if an error occurs building the SQL
     */
    private String buildFinalSql() throws SQLException {
        String sql = originalSql;
        
        for (Map.Entry<Integer, Object> entry : parameters.entrySet()) {
            int index = entry.getKey();
            Object value = entry.getValue();
            
            String placeholder = "\\?";
            String replacement;
            
            if (value == null) {
                replacement = "NULL";
            } else if (value instanceof String) {
                replacement = "'" + value.toString().replace("'", "''") + "'";
            } else if (value instanceof Date) {
                replacement = "'" + value.toString() + "'";
            } else if (value instanceof Time) {
                replacement = "'" + value.toString() + "'";
            } else if (value instanceof Timestamp) {
                replacement = "'" + value.toString() + "'";
            } else {
                replacement = value.toString();
            }
            
            sql = sql.replaceFirst(placeholder, replacement);
        }
        
        return sql;
    }
    
    /**
     * Builds a query to obtain metadata (adds LIMIT 0).
     */
    private String buildMetadataQuery() throws SQLException {
        String sql = buildFinalSql();
        
        if (!sql.toLowerCase().contains("limit")) {
            sql += " LIMIT 0";
        }
        
        return sql;
    }
} 