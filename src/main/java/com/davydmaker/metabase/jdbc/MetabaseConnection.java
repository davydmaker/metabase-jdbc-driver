package com.davydmaker.metabase.jdbc;

import com.davydmaker.metabase.api.MetabaseApiClient;
import com.davydmaker.metabase.api.MetabaseTableInfo;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Collections;
import java.util.concurrent.Executor;

/**
 * JDBC Connection implementation for Metabase driver.
 * 
 * Manages connections to Metabase instances and provides methods for creating
 * statements and managing database metadata with proactive caching.
 */
public class MetabaseConnection implements Connection {
    private final MetabaseApiClient apiClient;
    private final String databaseName;
    private final String url;
    
    private boolean closed = false;
    private boolean autoCommit = true;
    private boolean readOnly = false;
    private int transactionIsolation = Connection.TRANSACTION_NONE;
    private String catalog;
    private String schema;
    
    /**
     * Creates a new Metabase connection.
     * 
     * @param apiClient Metabase API client
     * @param databaseName Target database name
     * @param url Connection URL string
     * @throws SQLException If connection validation or cache initialization fails
     */
    public MetabaseConnection(MetabaseApiClient apiClient, String databaseName, String url) throws SQLException {
        this.apiClient = apiClient;
        this.databaseName = databaseName;
        this.url = url;
        this.catalog = databaseName;
        
        try {
            apiClient.validateDatabase(databaseName);
            
            apiClient.clearMetadataCache();
            
            apiClient.setSchemaDatabase(databaseName);
            
            List<String> schemas = apiClient.getSchemas();
            startBackgroundMetadataLoading(schemas);
            
        } catch (SQLException e) {
            throw new SQLException("Test Connection failed: " + e.getMessage());
        }
    }
    
    /**
     * Starts background metadata loading for improved autocomplete performance.
     * 
     * @param schemas List of schema names to preload
     */
    private void startBackgroundMetadataLoading(List<String> schemas) {
        Thread metadataThread = new Thread(() -> {
            try {
                int loadedSchemas = 0;
                int totalTables = 0;
                long startTime = System.currentTimeMillis();
                
                for (String schema : schemas) {
                    try {
                        List<MetabaseTableInfo> tables = apiClient.getTables(schema);
                        totalTables += tables.size();
                        loadedSchemas++;
                        
                        if (schemas.size() > 5) {
                            Thread.sleep(150);
                        }
                        
                    } catch (Exception e) {
                        // Silently ignore errors
                    }
                }
                
                long duration = System.currentTimeMillis() - startTime;   
            } catch (Exception e) {
                // Silently ignore errors
            }
        });
        
        metadataThread.setName("metabase-metadata-loader");
        metadataThread.setDaemon(true);
        metadataThread.setPriority(Thread.NORM_PRIORITY - 1);
        metadataThread.start();
    }
    
    @Override
    public Statement createStatement() throws SQLException {
        return new MetabaseStatement(this, apiClient, databaseName);
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new MetabasePreparedStatement(this, apiClient, databaseName, sql);
    }
    
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }
    
    @Override
    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }
    
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.autoCommit = autoCommit;
    }
    
    @Override
    public boolean getAutoCommit() throws SQLException {
        return autoCommit;
    }
    
    @Override
    public void commit() throws SQLException {
        if (autoCommit) {
            throw new SQLException("Cannot commit with auto-commit enabled");
        }
    }
    
    @Override
    public void rollback() throws SQLException {
        if (autoCommit) {
            throw new SQLException("Cannot rollback with auto-commit enabled");
        }
    }
    
    @Override
    public void close() throws SQLException {
        if (!closed) {
            
            // Release the shared connection
            if (apiClient != null) {
                apiClient.close();
            }
            
            closed = true;
        }
    }
    
    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }
    
    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new MetabaseDatabaseMetaData(this, apiClient, databaseName);
    }
    
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        this.readOnly = readOnly;
    }
    
    @Override
    public boolean isReadOnly() throws SQLException {
        return readOnly;
    }
    
    @Override
    public void setCatalog(String catalog) throws SQLException {
        this.catalog = catalog;
    }
    
    @Override
    public String getCatalog() throws SQLException {
        return catalog;
    }
    
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        this.transactionIsolation = level;
    }
    
    @Override
    public int getTransactionIsolation() throws SQLException {
        return transactionIsolation;
    }
    
    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }
    
    @Override
    public void clearWarnings() throws SQLException {
    }
    
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement();
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql);
    }
    
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }
    
    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return Collections.emptyMap();
    }
    
    @Override
    public void setTypeMap(Map<String, Class<?>> map) {
    }
    
    @Override
    public void setHoldability(int holdability) {
    }
    
    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }
    
    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }
    
    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }
    
    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }
    
    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }
    
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return createStatement();
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return prepareStatement(sql);
    }
    
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }
    
    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }
    
    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }
    
    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }
    
    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }
    
    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (closed) {
            return false;
        }
        
        try {
            if (apiClient != null && apiClient.isAuthenticated()) {
                try {
                    List<String> schemas = apiClient.getSchemas();
                    return true;
                } catch (Exception e) {
                    // If failed, try to reconnect
                    try {
                        apiClient.authenticate();
                        return true;
                    } catch (Exception reconnectError) {
                        return false;
                    }
                }
            }
            
            // If not authenticated, try to reconnect
            try {
                apiClient.authenticate();
                return true;
            } catch (Exception e) {
                return false;
            }
        } catch (Exception e) {
            return false;
        }
    }
    
    @Override
    public void setClientInfo(String name, String value) {
    }
    
    @Override
    public void setClientInfo(Properties properties) {
    }
    
    @Override
    public String getClientInfo(String name) {
        return null;
    }
    
    @Override
    public Properties getClientInfo() {
        return new Properties();
    }
    
    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array not supported");
    }
    
    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Struct not supported");
    }
    
    @Override
    public void setSchema(String schema) throws SQLException {
        this.schema = schema;
    }
    
    @Override
    public String getSchema() throws SQLException {
        return schema;
    }
    
    @Override
    public void abort(Executor executor) throws SQLException {
        close();
    }
    
    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) {
    }
    
    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
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
     * Checks if connection is closed and throws exception if so.
     * 
     * @throws SQLException If connection is closed
     */
    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed");
        }
        
        // Check if the shared connection is still valid
        if (apiClient != null && !apiClient.isAuthenticated()) {
            // Try to reconnect before marking as closed
            try {
                apiClient.authenticate();
            } catch (SQLException e) {
                // If reconnection fails, mark as closed
                closed = true;
                throw new SQLException("Connection is closed - authentication failed: " + e.getMessage());
            }
        }
    }
    
    /**
     * Gets Metabase API client instance.
     * 
     * @return API client
     */
    public MetabaseApiClient getApiClient() {
        return apiClient;
    }
    
    /**
     * Gets target database name.
     * 
     * @return Database name
     */
    public String getDatabaseName() {
        return databaseName;
    }
    
    /**
     * Gets connection URL.
     * 
     * @return Connection URL string
     */
    public String getUrl() {
        return url;
    }
} 