package com.davydmaker.metabase.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Global connection manager that shares authentication and cache between all instances of the driver.
 * 
 * This singleton saves resources by reusing authentication tokens and metadata caches
 * between multiple JDBC connections to the same Metabase server.
 * 
 */
public class MetabaseConnectionManager {
    // Singleton instance
    private static volatile MetabaseConnectionManager instance;
    private static final Object INSTANCE_LOCK = new Object();
    
    // Connection cache by key (host:port:username)
    private final Map<String, SharedConnection> sharedConnections = new ConcurrentHashMap<>();
    
    // Global HTTP client with optimized connection pool
    private final CloseableHttpClient globalHttpClient;
    private final ObjectMapper objectMapper;
    
    // Cleanup scheduler
    private final ScheduledExecutorService cleanupScheduler;
    
    // Session token TTL (default: 2 hours)
    private static final long TOKEN_TTL_MS = 2 * 60 * 60 * 1000;
    
    // Metadata cache TTL (default: 30 minutes)
    private static final long METADATA_TTL_MS = 30 * 60 * 1000;
    
    // Cleanup interval (default: 15 minutes)
    private static final long CLEANUP_INTERVAL_MS = 15 * 60 * 1000;

    private MetabaseConnectionManager() {
        this.objectMapper = new ObjectMapper();
        
        // Optimized HTTP client for multiple connections
        this.globalHttpClient = HttpClients.custom()
            .setDefaultRequestConfig(
                RequestConfig.custom()
                    .setConnectTimeout(5000, TimeUnit.MILLISECONDS)
                    .setResponseTimeout(15000, TimeUnit.MILLISECONDS)
                    .build()
            )
            .setConnectionManager(
                PoolingHttpClientConnectionManagerBuilder.create()
                    .setMaxConnTotal(100)      // More connections in the pool
                    .setMaxConnPerRoute(50)    // More connections per route
                    .build()
            )
            .build();
            
        // Initialize cleanup scheduler
        this.cleanupScheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "MetabaseConnectionManager-Cleanup");
            t.setDaemon(true); // Daemon thread to prevent JVM shutdown
            return t;
        });
        
        // Schedule cleanup every 15 minutes
        cleanupScheduler.scheduleWithFixedDelay(
            this::cleanupExpiredConnections,
            CLEANUP_INTERVAL_MS,
            CLEANUP_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        );
        
        // Shutdown hook for proper cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
            
        
    }
    
    /**
     * Gets the singleton instance of the manager.
     */
    public static MetabaseConnectionManager getInstance() {
        if (instance == null) {
            synchronized (INSTANCE_LOCK) {
                if (instance == null) {
                    instance = new MetabaseConnectionManager();
                }
            }
        }
        return instance;
    }
    
    /**
     * Gets a shared connection for the specified server.
     * If a valid connection already exists, it will be reused.
     * 
     * @param host Metabase server hostname
     * @param port Server port
     * @param username Username
     * @param password Password
     * @param useSSL Whether to use HTTPS
     * @param authTimeout Authentication timeout
     * @param maxRetries Maximum number of retries
     * @return Valid shared connection
     * @throws SQLException If authentication fails
     */
    public SharedConnection getSharedConnection(String host, int port, String username, 
                                              String password, boolean useSSL, 
                                              int authTimeout, int maxRetries) throws SQLException {
        
        String connectionKey = buildConnectionKey(host, port, username);
        
        SharedConnection shared = sharedConnections.get(connectionKey);
        
        // Check if the connection exists and is valid
        if (shared != null && shared.isValid()) {
            shared.incrementRefCount();
            return shared;
        }
        
        // Create new shared connection
        shared = createSharedConnection(host, port, username, password, useSSL, authTimeout, maxRetries);
        sharedConnections.put(connectionKey, shared);
        shared.incrementRefCount();
        
        return shared;
    }
    
    /**
     * Releases a reference to the shared connection.
     * When there are no more references, the connection can be cleaned up.
     */
    public void releaseSharedConnection(String host, int port, String username) {
        String connectionKey = buildConnectionKey(host, port, username);
        SharedConnection shared = sharedConnections.get(connectionKey);
        
        if (shared != null) {
            shared.decrementRefCount();
            
            // Only remove if there are no more references and the connection has expired
            // Active connections should be kept even without references for a while
            if (shared.getRefCount() <= 0 && !shared.isValid()) {
                sharedConnections.remove(connectionKey);
                
            }
        }
    }
    
    /**
     * Creates a new shared connection with authentication.
     */
    private SharedConnection createSharedConnection(String host, int port, String username, 
                                                  String password, boolean useSSL, 
                                                  int authTimeout, int maxRetries) throws SQLException {
        
        String protocol = useSSL ? "https" : "http";
        String baseUrl = String.format("%s://%s:%d", protocol, host, port);
        
        String token = authenticate(baseUrl, username, password, authTimeout, maxRetries);
        
        // Global metadata cache for this server
        GlobalMetadataCache metadataCache = new GlobalMetadataCache(baseUrl, token, globalHttpClient, objectMapper);
        
        return new SharedConnection(baseUrl, token, metadataCache, System.currentTimeMillis());
    }
    
    /**
     * Performs authentication on Metabase and returns the session token.
     */
    private String authenticate(String baseUrl, String username, String password, 
                              int authTimeout, int maxRetries) throws SQLException {
        
        SQLException lastException = null;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                HttpPost request = new HttpPost(baseUrl + "/api/session");
                
                MetabaseDriverInfo.setAuthenticationHeaders(request);
                
                Map<String, Object> authPayload = new HashMap<>();
                authPayload.put("username", username);
                authPayload.put("password", password);
                authPayload.put("client_info", MetabaseDriverInfo.createClientInfo());
                
                String jsonPayload = objectMapper.writeValueAsString(authPayload);
                request.setEntity(new StringEntity(jsonPayload, ContentType.APPLICATION_JSON));
                
                // Configure specific timeout for authentication
                request.setConfig(
                    RequestConfig.custom()
                        .setConnectTimeout(authTimeout, TimeUnit.MILLISECONDS)
                        .setResponseTimeout(authTimeout, TimeUnit.MILLISECONDS)
                        .build()
                );
                
                try (CloseableHttpResponse response = globalHttpClient.execute(request)) {
                    String responseBody = EntityUtils.toString(response.getEntity());
                    
                    if (response.getCode() != 200) {
                        throw new SQLException("Authentication failed: " + responseBody);
                    }
                    
                    JsonNode responseJson = objectMapper.readTree(responseBody);
                    String token = responseJson.get("id").asText();
                    
                    
                    return token;
                }
                
            } catch (IOException | ParseException e) {
                lastException = new SQLException("Communication error during authentication (attempt " + 
                                               attempt + "/" + maxRetries + "): " + e.getMessage(), e);
                
                
                if (attempt < maxRetries) {
                    try {
                        Thread.sleep(500 * attempt); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("Interrupted during retry", ie);
                    }
                }
            }
        }
        
        throw lastException;
    }
    
    /**
     * Cleans up expired connections (method called periodically).
     */
    public void cleanupExpiredConnections() {
        List<String> expiredKeys = new ArrayList<>();
        int totalConnections = sharedConnections.size();
        
        for (Map.Entry<String, SharedConnection> entry : sharedConnections.entrySet()) {
            SharedConnection connection = entry.getValue();
            
            // Only remove if the connection has expired and has no active references
            if (!connection.isValid() && connection.getRefCount() <= 0 && 
                (System.currentTimeMillis() - connection.getLastReleaseTime()) > 60000) { // 1 minute without references
                expiredKeys.add(entry.getKey());
            }
        }
        
        for (String key : expiredKeys) {
            sharedConnections.remove(key);
        }
    }
    
    /**
     * Graceful shutdown of the manager.
     */
    private void shutdown() {
        try {
            
            
            // Stop the scheduler
            cleanupScheduler.shutdown();
            
            // Wait for pending tasks (maximum 5 seconds)
            if (!cleanupScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupScheduler.shutdownNow();
            }
            
            // Close HTTP client
            if (globalHttpClient != null) {
                globalHttpClient.close();
            }
            
            // Clear connections
            sharedConnections.clear();
            
            
            
        } catch (Exception e) {
            
        }
    }
    
    /**
     * Builds a unique key to identify connections.
     */
    private String buildConnectionKey(String host, int port, String username) {
        return String.format("%s:%d:%s", host, port, username);
    }
    
    /**
     * Gets the global HTTP client.
     */
    public CloseableHttpClient getHttpClient() {
        return globalHttpClient;
    }
    
    /**
     * Gets the global ObjectMapper.
     */
    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }
    
    /**
     * Internal class to represent a shared connection.
     */
    public static class SharedConnection {
        private final String baseUrl;
        private final String token;
        private final GlobalMetadataCache metadataCache;
        private final long createdAt;
        private final ReentrantLock lock = new ReentrantLock();
        private volatile int refCount = 0;
        private volatile long lastReleaseTime = 0;
        
        public SharedConnection(String baseUrl, String token, GlobalMetadataCache metadataCache, long createdAt) {
            this.baseUrl = baseUrl;
            this.token = token;
            this.metadataCache = metadataCache;
            this.createdAt = createdAt;
        }
        
        public boolean isValid() {
            return (System.currentTimeMillis() - createdAt) < TOKEN_TTL_MS;
        }
        
        public void incrementRefCount() {
            lock.lock();
            try {
                refCount++;
            } finally {
                lock.unlock();
            }
        }
        
        public void decrementRefCount() {
            lock.lock();
            try {
                refCount = Math.max(0, refCount - 1);
                if (refCount == 0) {
                    lastReleaseTime = System.currentTimeMillis();
                }
            } finally {
                lock.unlock();
            }
        }
        
        public int getRefCount() {
            lock.lock();
            try {
                return refCount;
            } finally {
                lock.unlock();
            }
        }
        
        public long getLastReleaseTime() {
            return lastReleaseTime;
        }
        
        // Getters
        public String getBaseUrl() { return baseUrl; }
        public String getToken() { return token; }
        public GlobalMetadataCache getMetadataCache() { return metadataCache; }
        public long getCreatedAt() { return createdAt; }
    }
} 