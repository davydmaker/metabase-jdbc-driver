package com.davydmaker.metabase.api;

import java.util.Objects;

/**
 * Represents information about a table returned by the Metabase API.
 * 
 */
public class MetabaseTableInfo {
    
    private final Integer id;
    private final String name;
    private final String displayName;
    private final String schema;
    private final String entityType;
    private final String description;
    private final Integer viewCount;
    private final Long estimatedRowCount;
    
    public MetabaseTableInfo(Integer id, String name, String displayName, String schema, 
                           String entityType, String description, Integer viewCount, 
                           Long estimatedRowCount) {
        this.id = id;
        this.name = name;
        this.displayName = displayName;
        this.schema = schema;
        this.entityType = entityType;
        this.description = description;
        this.viewCount = viewCount;
        this.estimatedRowCount = estimatedRowCount;
    }
    
    /**
     * Get the table ID.
     */
    public Integer getId() {
        return id;
    }
    
    /**
     * Get the table name.
     */
    public String getName() {
        return name;
    }
    
    /**
     * Get the table display name.
     */
    public String getDisplayName() {
        return displayName;
    }
    
    /**
     * Get the table schema.
     */
    public String getSchema() {
        return schema;
    }
    
    /**
     * Get the table entity type.
     */
    public String getEntityType() {
        return entityType;
    }
    
    /**
     * Get the table description.
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * Get the estimated number of table rows.
     */
    public Long getEstimatedRowCount() {
        return estimatedRowCount;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetabaseTableInfo that = (MetabaseTableInfo) o;
        return Objects.equals(id, that.id) &&
               Objects.equals(name, that.name) &&
               Objects.equals(schema, that.schema);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id, name, schema);
    }
    
    @Override
    public String toString() {
        return String.format("MetabaseTableInfo{id=%d, name='%s', schema='%s', displayName='%s'}", 
                           id, name, schema, displayName);
    }
} 