package com.davydmaker.metabase.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

/**
 * HTTP client for Metabase API communication with global connection management.
 */
public class MetabaseApiClient {
    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final boolean useSSL;
    private final int connectTimeout;
    private final int requestTimeout;
    private final int queryTimeout;
    private final int metadataTimeout;
    private final int authTimeout;
    private final int maxRetries;
    
    private MetabaseConnectionManager.SharedConnection sharedConnection;
    
    private String baseUrl;
    private String metabaseToken;
    private GlobalMetadataCache globalCache;
    private CloseableHttpClient httpClient;
    private ObjectMapper objectMapper;
    
    private List<MetabaseDatabase> databases;
    private String currentDatabaseName;
    
    private volatile boolean cancelled = false;
    
    /**
     * Creates a new Metabase API client with configurable timeouts.
     */
    public MetabaseApiClient(String host, int port, String username, String password, 
                           boolean useSSL, int connectTimeout, int requestTimeout, 
                           int queryTimeout, int metadataTimeout, int authTimeout, int maxRetries) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.useSSL = useSSL;
        this.connectTimeout = connectTimeout;
        this.requestTimeout = requestTimeout;
        this.queryTimeout = queryTimeout;
        this.metadataTimeout = metadataTimeout;
        this.authTimeout = authTimeout;
        this.maxRetries = maxRetries;
        
        String protocol = useSSL ? "https" : "http";
        this.baseUrl = String.format("%s://%s:%d", protocol, host, port);
    }
    
    /**
     * Authenticates with Metabase using global connection manager.
     * Reuses existing tokens or creates new authentication if necessary.
     */
    public void authenticate() throws SQLException {
        try {
            MetabaseConnectionManager manager = MetabaseConnectionManager.getInstance();
            this.sharedConnection = manager.getSharedConnection(
                host, port, username, password, useSSL, authTimeout, maxRetries
            );
            
            this.baseUrl = sharedConnection.getBaseUrl();
            this.metabaseToken = sharedConnection.getToken();
            this.globalCache = sharedConnection.getMetadataCache();
            this.httpClient = manager.getHttpClient();
            this.objectMapper = manager.getObjectMapper();
            
            loadDatabases();
            
        } catch (SQLException e) {
            
            throw e;
        }
    }
    
    /**
     * Loads available databases from global cache.
     */
    private void loadDatabases() throws SQLException {
        this.databases = globalCache.getDatabases();
    }
    
    /**
     * Gets database ID by name.
     */
    public Integer getDatabaseIdByName(String databaseName) {
        if (databases == null || databaseName == null) {
            return null;
        }
        
        for (MetabaseDatabase db : databases) {
            if (databaseName.equals(db.getName())) {
                return db.getId();
            }
        }
        
        return null;
    }
    
    /**
     * Validates if database exists and is accessible.
     */
    public void validateDatabase(String databaseName) throws SQLException {
        Integer databaseId = getDatabaseIdByName(databaseName);
        if (databaseId == null) {
            throw new SQLException("Database '" + databaseName + "' not found");
        }
    }
    
    /**
     * Executes SQL query using shared connection.
     */
    public MetabaseQueryResult executeQuery(String query, String databaseName) throws SQLException {
        ensureConnection();
        
        Integer databaseId = getDatabaseIdByName(databaseName);
        if (databaseId == null) {
            throw new SQLException("Database '" + databaseName + "' not found");
        }
        
        try {
            HttpPost request = new HttpPost(baseUrl + "/api/dataset");
            MetabaseDriverInfo.setQueryHeaders(request);
            request.setHeader("X-Metabase-Session", metabaseToken);
            
            Map<String, Object> queryPayload = new HashMap<>();
            queryPayload.put("database", databaseId);
            queryPayload.put("type", "native");
            
            Map<String, String> nativeQuery = new HashMap<>();
            nativeQuery.put("query", query);
            queryPayload.put("native", nativeQuery);
            
            queryPayload.put("template-tags", new HashMap<>());
            queryPayload.put("query_metadata", MetabaseDriverInfo.createQueryMetadata(query.length()));
            
            String jsonPayload = objectMapper.writeValueAsString(queryPayload);
            request.setEntity(new StringEntity(jsonPayload, ContentType.APPLICATION_JSON));
            
            try (CloseableHttpResponse response = executeWithTimeoutQuery(request)) {
                String responseBody = EntityUtils.toString(response.getEntity());
                                
                if (response.getCode() != 200 && response.getCode() != 202) {
                    throw new SQLException("Query execution failed: " + responseBody);
                }
                
                JsonNode responseJson = objectMapper.readTree(responseBody);
                return MetabaseQueryResult.fromJson(responseJson, objectMapper);
            }
        } catch (IOException | ParseException e) {
            
            throw new SQLException("Error executing query: " + e.getMessage(), e);
        }
    }
    
    /**
     * Verifies if the connection is active and attempts to reestablish if necessary.
     */
    private void ensureConnection() throws SQLException {
        if (!isAuthenticated()) {
            
            authenticate();
        }
    }
    
    /**
     * Gets list of available schemas using global cache.
     */
    public List<String> getSchemas() throws SQLException {
        ensureConnection();
        
        if (databases == null || databases.isEmpty()) {
            throw new SQLException("No databases available");
        }
        
        if (currentDatabaseName == null) {
            throw new SQLException("No database configured for this connection");
        }
        
        Integer databaseId = getDatabaseIdByName(currentDatabaseName);
        if (databaseId == null) {
            throw new SQLException("Configured database '" + currentDatabaseName + "' not found");
        }
        
        return globalCache.getSchemas(databaseId);
    }
    
    /**
     * Gets tables for specified schema using global cache.
     */
    public List<MetabaseTableInfo> getTables(String schemaName) throws SQLException {
        ensureConnection();
        
        if (databases == null || databases.isEmpty()) {
            throw new SQLException("No databases available");
        }
        
        if (currentDatabaseName == null) {
            throw new SQLException("No database configured for this connection");
        }
        
        Integer databaseId = getDatabaseIdByName(currentDatabaseName);
        if (databaseId == null) {
            throw new SQLException("Configured database '" + currentDatabaseName + "' not found");
        }
        
        return globalCache.getTables(databaseId, schemaName);
    }
    
    /**
     * Gets column information for table using global cache.
     */
    public List<MetabaseColumnInfo> getColumns(Integer tableId) throws SQLException {
        ensureConnection();
        return globalCache.getColumns(tableId);
    }
    
    /**
     * Gets database by name (for compatibility).
     */
    public MetabaseDatabase getDatabaseByName(String databaseName) {
        if (databases == null || databaseName == null) {
            return null;
        }
        
        for (MetabaseDatabase db : databases) {
            if (databaseName.equals(db.getName())) {
                return db;
            }
        }
        
        return null;
    }
    
    /**
     * Gets table columns (alias for getColumns for compatibility).
     */
    public List<MetabaseColumnInfo> getTableColumns(Integer tableId) throws SQLException {
        return getColumns(tableId);
    }
    
    /**
     * Sets the schema database (for compatibility).
     */
    public void setSchemaDatabase(String databaseName) throws SQLException {
        validateDatabase(databaseName);
        this.currentDatabaseName = databaseName;
    }
    
    /**
     * Clears metadata cache (delegates to global cache).
     */
    public void clearMetadataCache() {
        if (globalCache != null) {
            globalCache.clearCache();
        }
    }
        
    private CloseableHttpResponse executeWithTimeout(HttpUriRequestBase request) throws SQLException {
        return executeWithTimeoutAndRetry(request, requestTimeout, "general request");
    }
    
    private CloseableHttpResponse executeWithTimeoutQuery(HttpUriRequestBase request) throws SQLException {
        return executeWithTimeoutAndRetry(request, queryTimeout, "SQL query execution");
    }
    
    private CloseableHttpResponse executeWithTimeoutAndRetry(HttpUriRequestBase request, 
                                                           int timeout, String operationType) throws SQLException {
        SQLException lastException = null;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                request.setConfig(
                    org.apache.hc.client5.http.config.RequestConfig.custom()
                        .setConnectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
                        .setResponseTimeout(timeout, TimeUnit.MILLISECONDS)
                        .build()
                );
                
                CloseableHttpResponse response = httpClient.execute(request);
                
                return response;
                
            } catch (java.net.SocketTimeoutException e) {
                lastException = new SQLException("Timeout on " + operationType + " (attempt " + attempt + "/" + maxRetries + ")", e);
                
                
                if (attempt < maxRetries) {
                    try {
                        Thread.sleep(1000 * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("Interrupted during retry", ie);
                    }
                }
                
            } catch (IOException e) {
                lastException = new SQLException("I/O error on " + operationType + " (attempt " + attempt + "/" + maxRetries + ")", e);
                
                
                if (attempt < maxRetries) {
                    try {
                        Thread.sleep(1000 * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("Interrupted during retry", ie);
                    }
                }
            }
        }
        
        throw lastException;
    }
    
    public void cancel() {
        this.cancelled = true;
    }
    
    public List<MetabaseDatabase> getDatabases() {
        return databases != null ? new ArrayList<>(databases) : new ArrayList<>();
    }
    
    public List<MetabaseDatabase> getAllDatabases() {
        return getDatabases();
    }
    
    public boolean isAuthenticated() {
        if (sharedConnection == null || metabaseToken == null) {
            return false;
        }
        
        return sharedConnection.isValid();
    }
    
    public String getBaseUrl() {
        return baseUrl;
    }
    
    public String getToken() {
        return metabaseToken;
    }
    
    /**
     * Releases the shared connection when this client is no longer needed.
     * Important: Call this method when closing the connection to properly manage resources.
     */
    public void close() {
        if (sharedConnection != null) {
            releaseSharedConnectionAsync();
        }
    }
    
    /**
     * Releases the shared connection asynchronously to allow 
     * ongoing metadata operations to complete.
     */
    private void releaseSharedConnectionAsync() {
        if (sharedConnection == null) return;
        
        final String hostRef = this.host;
        final int portRef = this.port;
        final String usernameRef = this.username;
        final MetabaseConnectionManager.SharedConnection connectionRef = this.sharedConnection;
        
        // Schedule release after a delay to allow metadata operations to complete
        // Don't clear references immediately to allow ongoing operations
        java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "MetabaseApiClient-Release");
            t.setDaemon(true);
            return t;
        }).schedule(() -> {
            try {
                // Only clear references after the delay
                if (sharedConnection == connectionRef) {
                    sharedConnection = null;
                    metabaseToken = null;
                    globalCache = null;
                    httpClient = null;
                    objectMapper = null;
                }
                
                MetabaseConnectionManager.getInstance().releaseSharedConnection(hostRef, portRef, usernameRef);
            } catch (Exception e) {
                
            }
        }, 10, java.util.concurrent.TimeUnit.SECONDS); // Increased delay to 10 seconds
    }
} 