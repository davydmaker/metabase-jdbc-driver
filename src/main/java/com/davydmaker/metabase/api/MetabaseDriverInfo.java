package com.davydmaker.metabase.api;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Centralized utility class for managing Metabase JDBC Driver information and HTTP headers.
 * 
 * This class contains all driver constants and provides methods to set standard headers
 * on HTTP requests to ensure consistency across the entire driver.
 * 
 */
public final class MetabaseDriverInfo {
    
    // Driver identification constants
    public static final String DRIVER_NAME = "Metabase JDBC Driver";
    public static final String DRIVER_VERSION = "1.2.1";
    public static final String DRIVER_VENDOR = "Davyd Maker";
    public static final String DRIVER_URL = "https://github.com/davydmaker/metabase-jdbc-driver";
    public static final String DRIVER_ICON_PATH = "/META-INF/metabase-driver-icon.svg";
    
    private static final String DRIVER_CLASS_NAME = "com.davydmaker.metabase.jdbc.MetabaseDriver";
    public static final String PROTOCOL_VERSION = "1.0";
    public static final String CLIENT_PACKAGE = "com.davydmaker.metabase";
    
    // Private constructor to prevent instantiation
    private MetabaseDriverInfo() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }
    
    /**
     * Gets the User-Agent string for HTTP requests.
     * 
     * @return Formatted User-Agent string with driver info and system details
     */
    public static String getUserAgent() {
        return DRIVER_NAME + "/" + DRIVER_VERSION + " (Java/" + System.getProperty("java.version") + "; " + 
               System.getProperty("os.name") + "/" + System.getProperty("os.version") + ")";
    }
    
    /**
     * Sets standard JDBC driver headers on HTTP requests.
     * 
     * @param request HTTP request to configure
     */
    public static void setStandardHeaders(HttpUriRequestBase request) {
        request.setHeader("Accept", "application/json");
        request.setHeader("User-Agent", getUserAgent());
        request.setHeader("X-Client-Type", "JDBC-Driver");
        request.setHeader("X-Client-Name", CLIENT_PACKAGE);
        request.setHeader("X-Client-Version", DRIVER_VERSION);
        request.setHeader("X-Protocol-Version", PROTOCOL_VERSION);
        request.setHeader("X-Driver-Vendor", DRIVER_VENDOR);
        request.setHeader("X-Request-Source", "jdbc-connection");
        request.setHeader("Cache-Control", "no-cache");
        request.setHeader("Pragma", "no-cache");
    }
    
    /**
     * Sets standard headers specifically for metadata requests.
     * 
     * @param request HTTP request to configure
     */
    public static void setMetadataHeaders(HttpGet request) {
        setStandardHeaders(request);
        request.setHeader("X-Request-Source", "jdbc-metadata");
    }
    
    /**
     * Sets enhanced headers for authentication requests.
     * 
     * @param request HTTP request to configure
     */
    public static void setAuthenticationHeaders(HttpPost request) {
        // Standard headers
        request.setHeader("Content-Type", "application/json");
        request.setHeader("Accept", "application/json");
        request.setHeader("User-Agent", getUserAgent());
        request.setHeader("X-Client-Type", "JDBC-Driver");
        request.setHeader("X-Client-Name", CLIENT_PACKAGE);
        request.setHeader("X-Client-Version", DRIVER_VERSION);
        request.setHeader("X-Protocol-Version", PROTOCOL_VERSION);
        request.setHeader("X-Driver-Vendor", DRIVER_VENDOR);
        request.setHeader("X-Connection-Type", "database-api");
        request.setHeader("X-Request-Source", "jdbc-connection");
        request.setHeader("Cache-Control", "no-cache");
        request.setHeader("Pragma", "no-cache");
        
        // Enhanced headers for debugging
        request.setHeader("X-Java-Version", System.getProperty("java.version"));
        request.setHeader("X-Java-Vendor", System.getProperty("java.vendor"));
        request.setHeader("X-OS-Name", System.getProperty("os.name"));
        request.setHeader("X-OS-Arch", System.getProperty("os.arch"));
    }
    
    /**
     * Sets headers for SQL query execution requests.
     * 
     * @param request HTTP request to configure
     */
    public static void setQueryHeaders(HttpPost request) {
        setStandardHeaders(request);
        request.setHeader("Content-Type", "application/json");
        request.setHeader("X-Request-Type", "sql-query");
        request.setHeader("X-Query-Source", "jdbc-statement");
    }
    
    /**
     * Sets headers for field values requests.
     * 
     * @param request HTTP request to configure
     * @param fieldId Field ID being requested
     * @param columnName Column name being requested
     */
    public static void setFieldValuesHeaders(HttpGet request, Integer fieldId, String columnName) {
        setStandardHeaders(request);
        request.setHeader("X-Request-Type", "field-values");
        request.setHeader("X-Field-Id", fieldId.toString());
        request.setHeader("X-Column-Name", columnName);
    }
    
    /**
     * Sets headers for schema metadata requests.
     * 
     * @param request HTTP request to configure
     * @param metadataType Type of metadata being requested
     */
    public static void setSchemaHeaders(HttpGet request, String metadataType) {
        setStandardHeaders(request);
        request.setHeader("X-Request-Type", "schema-metadata");
        request.setHeader("X-Metadata-Type", metadataType);
    }
    
    /**
     * Sets headers for table-specific metadata requests.
     * 
     * @param request HTTP request to configure
     * @param requestType Type of table request
     * @param tableId Table ID (optional)
     * @param schemaName Schema name (optional)
     */
    public static void setTableHeaders(HttpGet request, String requestType, String tableId, String schemaName) {
        setMetadataHeaders(request);
        request.setHeader("X-Request-Type", requestType);
        if (tableId != null) {
            request.setHeader("X-Table-Id", tableId);
        }
        if (schemaName != null) {
            request.setHeader("X-Schema-Name", schemaName);
        }
    }
    
    /**
     * Creates client information map for authentication.
     * 
     * @return Map with client identification information
     */
    public static Map<String, Object> createClientInfo() {
        Map<String, Object> clientInfo = new HashMap<>();
        clientInfo.put("driver_name", DRIVER_NAME);
        clientInfo.put("driver_version", DRIVER_VERSION);
        clientInfo.put("driver_vendor", DRIVER_VENDOR);
        clientInfo.put("java_version", System.getProperty("java.version"));
        clientInfo.put("os_name", System.getProperty("os.name"));
        clientInfo.put("os_version", System.getProperty("os.version"));
        clientInfo.put("client_type", "jdbc-driver");
        clientInfo.put("connection_time", System.currentTimeMillis());
        return clientInfo;
    }
    
    /**
     * Creates query metadata for tracking purposes.
     * 
     * @param queryLength Length of the SQL query
     * @return Map with query metadata
     */
    public static Map<String, Object> createQueryMetadata(int queryLength) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("query_length", queryLength);
        metadata.put("execution_time", System.currentTimeMillis());
        metadata.put("client_type", "jdbc-driver");
        metadata.put("driver_version", DRIVER_VERSION);
        return metadata;
    }
} 