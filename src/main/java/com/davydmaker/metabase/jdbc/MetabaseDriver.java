package com.davydmaker.metabase.jdbc;

import com.davydmaker.metabase.api.MetabaseApiClient;
import com.davydmaker.metabase.api.MetabaseDriverInfo;

import java.sql.*;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Custom JDBC driver to connect to Metabase via API.
 * 
 * This driver allows tools like DBeaver to connect to Metabase
 * and execute queries on databases connected to Metabase through the API.
 * 
 * Connection URL example:
 * jdbc:metabase://metabase-host:port/database-name?user=username&password=password
 * 
 */
public class MetabaseDriver implements Driver {
    private static final String URL_PREFIX = "jdbc:metabase://";
    private static final Pattern URL_PATTERN = Pattern.compile(
        "jdbc:metabase://([^:/]+)(?::(\\d+))?/([^?]+)(?:\\?(.+))?"
    );
    
    public static final int MAJOR_VERSION = Integer.parseInt(MetabaseDriverInfo.DRIVER_VERSION.split("\\.")[0]);
    public static final int MINOR_VERSION = Integer.parseInt(MetabaseDriverInfo.DRIVER_VERSION.split("\\.")[1]);
    
    static {
        try {
            DriverManager.registerDriver(new MetabaseDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Could not register MetabaseDriver", e);
        }
    }
    
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        
        try {
            ConnectionInfo connInfo = parseUrl(url, info);
            
            MetabaseApiClient apiClient = new MetabaseApiClient(
                connInfo.host, 
                connInfo.port, 
                connInfo.username, 
                connInfo.password, 
                connInfo.useSSL,
                connInfo.connectTimeout,
                connInfo.requestTimeout,
                connInfo.queryTimeout,
                connInfo.metadataTimeout,
                connInfo.authTimeout,
                connInfo.maxRetries
            );
            
            apiClient.authenticate();
            apiClient.validateDatabase(connInfo.databaseName);
            
            return new MetabaseConnection(apiClient, connInfo.databaseName, url);
        } catch (SQLException e) {
            throw e;
        }
    }
    
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith(URL_PREFIX);
    }
    
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }
    
    @Override
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }
    
    @Override
    public int getMinorVersion() {
        return MINOR_VERSION;
    }
    
    @Override
    public boolean jdbcCompliant() {
        return false;
    }
    
    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("getParentLogger method not supported");
    }
    
    /**
     * Parses the connection URL and extracts the necessary information.
     * 
     * @param url the JDBC URL
     * @param info additional connection properties
     * @return parsed connection information
     * @throws SQLException if the URL is invalid
     */
    private ConnectionInfo parseUrl(String url, Properties info) throws SQLException {
        Matcher matcher = URL_PATTERN.matcher(url);
        
        if (!matcher.matches()) {
            throw new SQLException("Invalid connection URL: " + url);
        }
        
        ConnectionInfo connInfo = new ConnectionInfo();
        
        connInfo.host = matcher.group(1);
        if (connInfo.host == null || connInfo.host.trim().isEmpty()) {
            throw new SQLException("Host not specified in URL");
        }
        
        connInfo.databaseName = matcher.group(3);
        if (connInfo.databaseName == null || connInfo.databaseName.trim().isEmpty()) {
            throw new SQLException("Database name not specified in URL");
        }
        
        String queryString = matcher.group(4);
        Properties urlParams = parseQueryString(queryString);
        
        Properties allProps = new Properties();
        if (info != null) {
            allProps.putAll(info);
        }
        allProps.putAll(urlParams);
        
        connInfo.useSSL = true;
        
        String portStr = matcher.group(2);
        if (portStr != null) {
            connInfo.port = Integer.parseInt(portStr);
        } else {
            connInfo.port = 443;
        }
        
        connInfo.username = allProps.getProperty("user");
        connInfo.password = allProps.getProperty("password");
        
        if (connInfo.username == null || connInfo.username.trim().isEmpty()) {
            throw new SQLException("Username not specified");
        }
        if (connInfo.password == null || connInfo.password.trim().isEmpty()) {
            throw new SQLException("Password not specified");
        }
        
        return connInfo;
    }
    
    /**
     * Parses the query string parameters.
     */
    private Properties parseQueryString(String queryString) {
        Properties props = new Properties();
        
        if (queryString != null && !queryString.trim().isEmpty()) {
            String[] pairs = queryString.split("&");
            for (String pair : pairs) {
                String[] keyValue = pair.split("=", 2);
                if (keyValue.length == 2) {
                    props.setProperty(keyValue[0], keyValue[1]);
                }
            }
        }
        
        return props;
    }

    /**
     * Parses a timeout parameter from the properties.
     * If the parameter is not found or is invalid, it returns the default value.
     */
    private int parseTimeoutParameter(Properties props, String paramName, int defaultValue) {
        String paramValue = props.getProperty(paramName);
        if (paramValue != null && !paramValue.trim().isEmpty()) {
            try {
                return Integer.parseInt(paramValue);
            } catch (NumberFormatException e) {
            }
        }
        return defaultValue;
    }
    
    /**
     * Inner class to store connection information.
     */
    private static class ConnectionInfo {
        String host;
        int port; // Inferred automatically: 443 for SSL, 80 for non-SSL
        String databaseName;
        String username;
        String password;
        boolean useSSL = true; // Default: true (HTTPS)
        int timeout = 30;
        int maxRetries = 2;     
        
        // Configurable specific timeouts
        int connectTimeout = 5000;
        int requestTimeout = 15000;
        int queryTimeout = 60000;
        int metadataTimeout = 10000;
        int authTimeout = 8000;
    }
} 