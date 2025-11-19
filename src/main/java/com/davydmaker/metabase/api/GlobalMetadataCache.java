package com.davydmaker.metabase.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Global cache of metadata shared between all connections to the same Metabase server.
 * 
 * This cache stores information about schemas, tables, and columns that are shared
 * between multiple instances of the driver, avoiding unnecessary metadata loads.
 * 
 */
public class GlobalMetadataCache {
    private final String baseUrl;
    private final String token;
    private final CloseableHttpClient httpClient;
    private final ObjectMapper objectMapper;
    
    // TTL for metadata cache (default: 30 minutes)
    private static final long METADATA_TTL_MS = 30 * 60 * 1000;
    
    // Available databases cache
    private volatile List<MetabaseDatabase> databases;
    private volatile long databasesCacheTime = 0;
    
    // Schemas cache by database ID
    private final Map<Integer, CachedSchemas> schemasCache = new ConcurrentHashMap<>();
    
    // Tables cache by database ID and schema
    private final Map<String, CachedTables> tablesCache = new ConcurrentHashMap<>();
    
    // Table details cache by table ID
    private final Map<Integer, CachedTableInfo> tableInfoCache = new ConcurrentHashMap<>();
    
    // Columns cache by table ID
    private final Map<Integer, CachedColumns> columnsCache = new ConcurrentHashMap<>();
    
    // Locks for read/write synchronization
    private final ReentrantReadWriteLock databasesLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock schemasLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock tablesLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock tableInfoLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock columnsLock = new ReentrantReadWriteLock();
    
    public GlobalMetadataCache(String baseUrl, String token, CloseableHttpClient httpClient, ObjectMapper objectMapper) {
        this.baseUrl = baseUrl;
        this.token = token;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
    }
    
    /**
     * Gets the list of available databases.
     */
    public List<MetabaseDatabase> getDatabases() throws SQLException {
        databasesLock.readLock().lock();
        try {
            if (databases != null && isCacheValid(databasesCacheTime)) {
                return new ArrayList<>(databases);
            }
        } finally {
            databasesLock.readLock().unlock();
        }
        
        // Cache expired or does not exist - load
        databasesLock.writeLock().lock();
        try {
            // Double-check after obtaining write lock
            if (databases == null || !isCacheValid(databasesCacheTime)) {
                loadDatabases();
            }
            return new ArrayList<>(databases);
        } finally {
            databasesLock.writeLock().unlock();
        }
    }
    
    /**
     * Gets the list of schemas for a database.
     */
    public List<String> getSchemas(Integer databaseId) throws SQLException {
        schemasLock.readLock().lock();
        try {
            CachedSchemas cached = schemasCache.get(databaseId);
            if (cached != null && isCacheValid(cached.cacheTime)) {
                return new ArrayList<>(cached.schemas);
            }
        } finally {
            schemasLock.readLock().unlock();
        }
        
        // Cache expired or does not exist - load
        schemasLock.writeLock().lock();
        try {
            CachedSchemas cached = schemasCache.get(databaseId);
            if (cached == null || !isCacheValid(cached.cacheTime)) {
                loadSchemas(databaseId);
                cached = schemasCache.get(databaseId);
            }
            return new ArrayList<>(cached.schemas);
        } finally {
            schemasLock.writeLock().unlock();
        }
    }
    
    /**
     * Gets the list of tables for a database and schema.
     */
    public List<MetabaseTableInfo> getTables(Integer databaseId, String schemaName) throws SQLException {
        String key = buildTablesCacheKey(databaseId, schemaName);
        
        tablesLock.readLock().lock();
        try {
            CachedTables cached = tablesCache.get(key);
            if (cached != null && isCacheValid(cached.cacheTime)) {
                return new ArrayList<>(cached.tables);
            }
        } finally {
            tablesLock.readLock().unlock();
        }
        
        // Cache expired or does not exist - load
        tablesLock.writeLock().lock();
        try {
            CachedTables cached = tablesCache.get(key);
            if (cached == null || !isCacheValid(cached.cacheTime)) {
                loadTables(databaseId, schemaName);
                cached = tablesCache.get(key);
            }
            return new ArrayList<>(cached.tables);
        } finally {
            tablesLock.writeLock().unlock();
        }
    }
    
    /**
     * Gets the list of columns for a table.
     */
    public List<MetabaseColumnInfo> getColumns(Integer tableId) throws SQLException {
        columnsLock.readLock().lock();
        try {
            CachedColumns cached = columnsCache.get(tableId);
            if (cached != null && isCacheValid(cached.cacheTime)) {
                return new ArrayList<>(cached.columns);
            }
        } finally {
            columnsLock.readLock().unlock();
        }
        
        // Cache expired or does not exist - load
        columnsLock.writeLock().lock();
        try {
            CachedColumns cached = columnsCache.get(tableId);
            if (cached == null || !isCacheValid(cached.cacheTime)) {
                loadColumns(tableId);
                cached = columnsCache.get(tableId);
            }
            return new ArrayList<>(cached.columns);
        } finally {
            columnsLock.writeLock().unlock();
        }
    }
    
    /**
     * Clears the entire cache (force reload).
     */
    public void clearCache() {
        databasesLock.writeLock().lock();
        try {
            databases = null;
            databasesCacheTime = 0;
        } finally {
            databasesLock.writeLock().unlock();
        }
        
        schemasCache.clear();
        tablesCache.clear();
        tableInfoCache.clear();
        columnsCache.clear();
        
        
    }
    
    private void loadDatabases() throws SQLException {
        try {
            String url = baseUrl + "/api/database";
            HttpGet request = new HttpGet(url);
            MetabaseDriverInfo.setStandardHeaders(request);
            request.setHeader("X-Metabase-Session", token);
            
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                if (response.getCode() != 200) {
                    throw new SQLException("Error loading databases: HTTP " + response.getCode());
                }
                
                String responseBody = EntityUtils.toString(response.getEntity());
                JsonNode responseJson = objectMapper.readTree(responseBody);
                JsonNode dataArray = responseJson.get("data");
                
                if (dataArray == null || !dataArray.isArray()) {
                    throw new SQLException("Invalid response format for databases");
                }
                
                List<MetabaseDatabase> databaseList = new ArrayList<>();
                for (JsonNode dbNode : dataArray) {
                    Integer id = dbNode.get("id").asInt();
                    String name = dbNode.get("name").asText();
                    String engine = dbNode.has("engine") ? dbNode.get("engine").asText() : "unknown";
                    
                    databaseList.add(new MetabaseDatabase(id, name, engine));
                }
                
                this.databases = databaseList;
                this.databasesCacheTime = System.currentTimeMillis();
            }
        } catch (IOException | ParseException e) {
            throw new SQLException("Error loading databases: " + e.getMessage(), e);
        }
    }
    
    private void loadSchemas(Integer databaseId) throws SQLException {
        try {
            String url = baseUrl + "/api/database/" + databaseId + "/schemas";
            HttpGet request = new HttpGet(url);
            MetabaseDriverInfo.setStandardHeaders(request);
            request.setHeader("X-Metabase-Session", token);
            
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                if (response.getCode() != 200) {
                    throw new SQLException("Error loading schemas: HTTP " + response.getCode());
                }
                
                String responseBody = EntityUtils.toString(response.getEntity());
                JsonNode jsonArray = objectMapper.readTree(responseBody);
                
                List<String> schemas = new ArrayList<>();
                if (jsonArray.isArray()) {
                    for (JsonNode schemaNode : jsonArray) {
                        schemas.add(schemaNode.asText());
                    }
                }
                
                schemasCache.put(databaseId, new CachedSchemas(schemas, System.currentTimeMillis()));
            }
        } catch (IOException | ParseException e) {
            throw new SQLException("Error loading schemas: " + e.getMessage(), e);
        }
    }
    
    private void loadTables(Integer databaseId, String schemaName) throws SQLException {
        try {
            String url = baseUrl + "/api/database/" + databaseId + "/schema/" + schemaName;
            HttpGet request = new HttpGet(url);
            MetabaseDriverInfo.setStandardHeaders(request);
            request.setHeader("X-Metabase-Session", token);
            
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                if (response.getCode() != 200) {
                    throw new SQLException("Error loading tables: HTTP " + response.getCode());
                }
                
                String responseBody = EntityUtils.toString(response.getEntity());
                JsonNode jsonArray = objectMapper.readTree(responseBody);
                
                List<MetabaseTableInfo> tables = new ArrayList<>();
                if (jsonArray.isArray()) {
                    for (JsonNode tableNode : jsonArray) {
                        MetabaseTableInfo table = parseTableFromJson(tableNode);
                        tables.add(table);
                    }
                }
                
                String key = buildTablesCacheKey(databaseId, schemaName);
                tablesCache.put(key, new CachedTables(tables, System.currentTimeMillis()));
            }
        } catch (IOException | ParseException e) {
            throw new SQLException("Error loading tables: " + e.getMessage(), e);
        }
    }
    
    private void loadTableInfo(Integer tableId) throws SQLException {
        try {
            String url = baseUrl + "/api/table/" + tableId;
            HttpGet request = new HttpGet(url);
            MetabaseDriverInfo.setStandardHeaders(request);
            request.setHeader("X-Metabase-Session", token);
            
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                if (response.getCode() != 200) {
                    throw new SQLException("Error loading table info: HTTP " + response.getCode());
                }
                
                String responseBody = EntityUtils.toString(response.getEntity());
                JsonNode tableNode = objectMapper.readTree(responseBody);
                
                MetabaseTableInfo tableInfo = parseTableFromJson(tableNode);
                tableInfoCache.put(tableId, new CachedTableInfo(tableInfo, System.currentTimeMillis()));
            }
        } catch (IOException | ParseException e) {
            throw new SQLException("Error loading table info: " + e.getMessage(), e);
        }
    }
    
    private void loadColumns(Integer tableId) throws SQLException {
        try {
            String url = baseUrl + "/api/table/" + tableId + "/query_metadata";
            HttpGet request = new HttpGet(url);
            MetabaseDriverInfo.setStandardHeaders(request);
            request.setHeader("X-Metabase-Session", token);
            
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                if (response.getCode() != 200) {
                    throw new SQLException("Error loading columns: HTTP " + response.getCode());
                }
                
                String responseBody = EntityUtils.toString(response.getEntity());
                JsonNode responseJson = objectMapper.readTree(responseBody);
                
                List<MetabaseColumnInfo> columns = new ArrayList<>();
                JsonNode fieldsArray = responseJson.get("fields");
                
                if (fieldsArray != null && fieldsArray.isArray()) {
                    for (JsonNode fieldNode : fieldsArray) {
                        // Filter only fields with nfc_path null (main table fields)
                        JsonNode nfcPathNode = fieldNode.get("nfc_path");
                        if (!fieldNode.has("nfc_path") || nfcPathNode == null || nfcPathNode.isNull()) {
                            MetabaseColumnInfo column = parseColumnFromJson(fieldNode);
                            columns.add(column);
                        }
                    }
                }
                
                columnsCache.put(tableId, new CachedColumns(columns, System.currentTimeMillis()));
            }
        } catch (IOException | ParseException e) {
            throw new SQLException("Error loading columns: " + e.getMessage(), e);
        }
    }
    
    private boolean isCacheValid(long cacheTime) {
        return (System.currentTimeMillis() - cacheTime) < METADATA_TTL_MS;
    }
    
    private String buildTablesCacheKey(Integer databaseId, String schemaName) {
        return databaseId + ":" + schemaName;
    }
    
    private MetabaseTableInfo parseTableFromJson(JsonNode tableNode) {
        Integer id = tableNode.get("id").asInt();
        String name = tableNode.get("name").asText();
        String displayName = tableNode.has("display_name") ? tableNode.get("display_name").asText() : name;
        String schema = tableNode.has("schema") ? tableNode.get("schema").asText() : "";
        String entityType = tableNode.has("entity_type") && !tableNode.get("entity_type").isNull() ? 
                           tableNode.get("entity_type").asText() : null;
        String description = tableNode.has("description") && !tableNode.get("description").isNull() ? 
                           tableNode.get("description").asText() : "";
        Integer viewCount = tableNode.has("view_count") && !tableNode.get("view_count").isNull() ? 
                           tableNode.get("view_count").asInt() : null;
        Long estimatedRowCount = tableNode.has("estimated_row_count") && !tableNode.get("estimated_row_count").isNull() ? 
                               tableNode.get("estimated_row_count").asLong() : null;
        
        return new MetabaseTableInfo(id, name, displayName, schema, entityType, description, 
                                   viewCount, estimatedRowCount);
    }
    
    private MetabaseColumnInfo parseColumnFromJson(JsonNode fieldNode) {
        Integer id = fieldNode.get("id").asInt();
        String name = fieldNode.get("name").asText();
        String displayName = fieldNode.has("display_name") ? fieldNode.get("display_name").asText() : name;
        String baseType = fieldNode.has("base_type") ? fieldNode.get("base_type").asText() : "unknown";
        String semanticType = fieldNode.has("semantic_type") && !fieldNode.get("semantic_type").isNull() ? 
                           fieldNode.get("semantic_type").asText() : null;
        String description = fieldNode.has("description") && !fieldNode.get("description").isNull() ? 
                           fieldNode.get("description").asText() : "";
        String databaseType = fieldNode.has("database_type") && !fieldNode.get("database_type").isNull() ? 
                            fieldNode.get("database_type").asText() : null;
        String effectiveType = fieldNode.has("effective_type") && !fieldNode.get("effective_type").isNull() ? 
                             fieldNode.get("effective_type").asText() : null;
        Long fieldId = id != null ? id.longValue() : null;
        
        return new MetabaseColumnInfo(name, displayName, baseType, semanticType, databaseType, effectiveType, fieldId);
    }
    
    private static class CachedSchemas {
        final List<String> schemas;
        final long cacheTime;
        
        CachedSchemas(List<String> schemas, long cacheTime) {
            this.schemas = Collections.unmodifiableList(new ArrayList<>(schemas));
            this.cacheTime = cacheTime;
        }
    }
    
    private static class CachedTables {
        final List<MetabaseTableInfo> tables;
        final long cacheTime;
        
        CachedTables(List<MetabaseTableInfo> tables, long cacheTime) {
            this.tables = Collections.unmodifiableList(new ArrayList<>(tables));
            this.cacheTime = cacheTime;
        }
    }
    
    private static class CachedTableInfo {
        final MetabaseTableInfo tableInfo;
        final long cacheTime;
        
        CachedTableInfo(MetabaseTableInfo tableInfo, long cacheTime) {
            this.tableInfo = tableInfo;
            this.cacheTime = cacheTime;
        }
    }
    
    private static class CachedColumns {
        final List<MetabaseColumnInfo> columns;
        final long cacheTime;
        
        CachedColumns(List<MetabaseColumnInfo> columns, long cacheTime) {
            this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
            this.cacheTime = cacheTime;
        }
    }
} 