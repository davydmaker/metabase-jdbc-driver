package com.davydmaker.metabase.api;

import java.util.Objects;

/**
 * Represents a connected database to Metabase.
 * 
 */
public class MetabaseDatabase {
    
    private final int id;
    private final String name;
    private final String engine;
    
    /**
     * Full constructor for MetabaseDatabase.
     * 
     * @param id ID of the database in Metabase
     * @param name Name of the database
     * @param engine Engine of the database (postgres, mysql, etc.)
     */
    public MetabaseDatabase(int id, String name, String engine) {
        this.id = id;
        this.name = name;
        this.engine = engine;
    }
    
    /**
     * Gets the ID of the database.
     */
    public int getId() {
        return id;
    }
    
    /**
     * Gets the name of the database.
     */
    public String getName() {
        return name;
    }
    
    /**
     * Gets the engine of the database.
     */
    public String getEngine() {
        return engine;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetabaseDatabase that = (MetabaseDatabase) o;
        return id == that.id &&
               Objects.equals(name, that.name) &&
               Objects.equals(engine, that.engine);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id, name, engine);
    }
    
    @Override
    public String toString() {
        return String.format("MetabaseDatabase{id=%d, name='%s', engine='%s'}", 
                           id, name, engine);
    }
} 