package com.davydmaker.metabase.api;

import java.sql.Types;
import java.util.Objects;

/**
 * Represents information about a column returned by the Metabase API.
 * 
 */
public class MetabaseColumnInfo {
    
    private final String name;
    private final String displayName;
    private final String baseType;
    private final String semanticType;
    private final String databaseType; // native database type (ex: "uuid", "jsonb", "varchar")
    private final String effectiveType; // effective type of Metabase
    private final Long fieldId; // ID of the field in Metabase to fetch values
    
    /**
     * Basic constructor (maintains compatibility).
     */
    public MetabaseColumnInfo(String name, String displayName, String baseType, String semanticType) {
        this(name, displayName, baseType, semanticType, null, null, null);
    }
    
    /**
     * Constructor with all types.
     */
    public MetabaseColumnInfo(String name, String displayName, String baseType, String semanticType, 
                             String databaseType, String effectiveType) {
        this(name, displayName, baseType, semanticType, databaseType, effectiveType, null);
    }
    
    /**
     * Constructor with all types and statistical metadata.
     * 
     * @param name Internal column name
     * @param displayName Display name of the column
     * @param baseType Base type of the column in Metabase
     * @param semanticType Semantic type of the column (can be null)
     * @param databaseType Native database type (can be null)
     * @param effectiveType Effective type of Metabase (can be null)
     * @param fieldId ID of the field in Metabase (can be null)
     */
    public MetabaseColumnInfo(String name, String displayName, String baseType, String semanticType, 
                             String databaseType, String effectiveType, Long fieldId) {
        this.name = name;
        this.displayName = displayName;
        this.baseType = baseType;
        this.semanticType = semanticType;
        this.databaseType = databaseType;
        this.effectiveType = effectiveType;
        this.fieldId = fieldId;
    }
    
    /**
     * Gets the column name.
     */
    public String getName() {
        return name;
    }
    
    /**
     * Gets the display name of the column.
     */
    public String getDisplayName() {
        return displayName;
    }
    
    /**
     * Gets the base type of the column.
     */
    public String getBaseType() {
        return baseType;
    }
    
    /**
     * Gets the semantic type of the column.
     */
    public String getSemanticType() {
        return semanticType;
    }
    
    /**
     * Gets the native database type.
     */
    public String getDatabaseType() {
        return databaseType;
    }
    
    /**
     * Gets the effective type of Metabase.
     */
    public String getEffectiveType() {
        return effectiveType;
    }
    
    /**
     * Gets the ID of the field in Metabase.
     */
    public Long getFieldId() {
        return fieldId;
    }
    
    /**
     * Checks if the type is a PostgreSQL ENUM.
     */
    public boolean isPostgreSQLEnum() {
        return effectiveType != null && 
               effectiveType.equals("type/PostgresEnum");
    }
    
    /**
     * Extracts the name of the PostgreSQL ENUM type.
     */
    public String getEnumTypeName() {
        if (!isPostgreSQLEnum()) {
            return null;
        }
        
        // Remove quotes and extract name: "webhook"."webhook_scope" -> webhook_scope
        String cleanType = databaseType.replace("\"", "");
        int dotIndex = cleanType.lastIndexOf(".");
        return dotIndex >= 0 ? cleanType.substring(dotIndex + 1) : cleanType;
    }
    
    /**
     * Converts the Metabase type to a JDBC SQL type.
     * Prioritizes database_type, then effective_type, then base_type.
     */
    public int getJdbcType() {
        // SPECIAL PRIORITY: UUID
        if (isUUID()) {
            return Types.OTHER; // UUIDs as OTHER for better recognition
        }

        // PRIORITY 1: database_type (native database type)
        if (databaseType != null) {
            String dbType = databaseType.toLowerCase();
            
            // Checks if it's a PostgreSQL ENUM
            if (isPostgreSQLEnum()) {
                return Types.VARCHAR; // ENUMs are treated as VARCHAR
            }

            switch (dbType) {
                case "bool":
                case "boolean":
                    return Types.BOOLEAN;
                case "uuid":
                    return Types.OTHER; // PostgreSQL UUID as OTHER
                case "jsonb":
                case "json":
                    return Types.OTHER; // PostgreSQL JSON as OTHER (native driver)
                case "timestamptz":
                case "timestamp with time zone":
                    return Types.TIMESTAMP_WITH_TIMEZONE;
                case "timestamp":
                case "timestamp without time zone":
                    return Types.TIMESTAMP;
                case "varchar":
                case "text":
                    return Types.VARCHAR;
                case "int4":
                case "integer":
                    return Types.INTEGER;
                case "int8":
                case "bigint":
                    return Types.BIGINT;
                case "numeric":
                case "decimal":
                    return Types.DECIMAL;
                case "bytea":
                    return Types.BLOB;
                case "date":
                    return Types.DATE;
                case "time":
                    return Types.TIME;
                case "float4":
                case "real":
                    return Types.REAL;
                case "float8":
                case "double precision":
                    return Types.DOUBLE;
                default: // For other database types (including enums), treat as VARCHAR
                    return Types.VARCHAR;
            }
        }
        
        // PRIORITY 2: effective_type (more precise, used internally by Metabase)
        if (effectiveType != null) {
            String effType = effectiveType.toLowerCase();
            
            switch (effType) {
                case "type/boolean":
                    return Types.BOOLEAN;
                case "type/uuid":
                    return Types.OTHER; // UUIDs as OTHER
                case "type/postgresenum":
                    return Types.VARCHAR; // PostgreSQL ENUMs as VARCHAR
                case "type/datetimewithlocaltz":
                case "type/datetime":
                    return Types.TIMESTAMP_WITH_TIMEZONE;
                case "type/date":
                    return Types.DATE;
                case "type/time":
                    return Types.TIME;
                case "type/text":
                    return Types.VARCHAR;
                case "type/integer":
                case "type/biginteger":
                    return Types.INTEGER;
                case "type/decimal":
                case "type/float":
                    return Types.DECIMAL;
                case "type/blob":
                    return Types.BLOB;
                case "type/json":
                    return Types.OTHER; // JSON as OTHER
            }
        }
        
        // PRIORITY 3: base_type (fallback)
        if (baseType != null) {
            String bType = baseType.toLowerCase();
            
            switch (bType) {
                case "type/boolean":
                    return Types.BOOLEAN;
                case "type/biginteger":
                case "type/integer":
                    return Types.INTEGER;
                case "type/decimal":
                case "type/float":
                    return Types.DECIMAL;
                case "type/text":
                case "type/string":
                    return Types.VARCHAR;
                case "type/datetime":
                case "type/date":
                case "type/datetimewithlocaltz":
                    return Types.TIMESTAMP;
                case "type/time":
                    return Types.TIME;
                case "type/blob":
                    return Types.BLOB;
                case "type/uuid":
                    return Types.OTHER; // UUIDs are treated as OTHER
                case "type/json":
                    return Types.OTHER; // JSON is treated as OTHER (PostgreSQL driver)
                case "type/postgresenum":
                    return Types.VARCHAR; // PostgreSQL ENUMs as VARCHAR
                case "type/*":
                    return Types.VARCHAR; // Generic type treated as string
            }
        }
        
        // Final fallback
        return Types.VARCHAR;
    }
    
    /**
     * Gets the Java class name corresponding to the column type.
     */
    public String getJavaTypeName() {
        switch (getJdbcType()) {
            case Types.INTEGER:
                return "java.lang.Integer";
            case Types.BIGINT:
                return "java.lang.Long";
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.REAL:
                return "java.math.BigDecimal";
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.LONGVARCHAR:
                return "java.lang.String";
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return "java.sql.Timestamp";
            case Types.DATE:
                return "java.sql.Date";
            case Types.TIME:
                return "java.sql.Time";
            case Types.BOOLEAN:
                return "java.lang.Boolean";
            case Types.BLOB:
                return "java.sql.Blob";
            case Types.OTHER:
                // For OTHER types (JSON/JSONB), returns String but allows IDEs to identify
                return "java.lang.String";
            default:
                return "java.lang.String";
        }
    }
    
    /**
     * Checks if the column is a primary key.
     */
    public boolean isPrimaryKey() {
        if (semanticType != null) {
            String semType = semanticType.toLowerCase();
            return semType.contains("type/pk");
        }
        
        return false;
    }
    
    /**
     * Checks if the column is a UUID.
     */
    public boolean isUUID() {
        // PRIORITY 1: database_type
        if (databaseType != null && databaseType.toLowerCase().equals("uuid")) {
            return true;
        }
        
        // PRIORITY 2: effective_type
        if (effectiveType != null && effectiveType.toLowerCase().equals("type/uuid")) {
            return true;
        }
        
        // PRIORITY 3: semantic_type
        if (semanticType != null) {
            String semType = semanticType.toLowerCase();
            if (semType.contains("uuid") || semType.contains("guid")) {
                return true;
            }
        }
        
        // PRIORITY 4: base_type
        if (baseType != null && baseType.toLowerCase().equals("type/uuid")) {
            return true;
        }
        
        return false;
    }
    
    /**
     * Gets the display type name (better description).
     * New hierarchy: database_type is sovereign, then effective_type, then base_type.
     */
    public String getDisplayTypeName() {
        // SPECIAL PRIORITY: UUID
        if (isUUID()) {
            return "uuid";
        }

        // PRIORITY 1: database_type (sovereign - native database type)
        if (databaseType != null) {
            // Checks if it's a PostgreSQL ENUM
            if (isPostgreSQLEnum()) {
                String enumName = getEnumTypeName();
                return enumName != null ? enumName.toLowerCase() : "enum";
            }

            return databaseType.toLowerCase();
        }
        
        // PRIORITY 2: effective_type (convert to database type)
        if (effectiveType != null) {
            String effType = effectiveType.toLowerCase();
            switch (effType) {
                case "type/boolean":
                    return "boolean";
                case "type/uuid":
                    return "uuid";
                case "type/postgresenum":
                    return "enum";
                case "type/datetimewithlocaltz":
                    return "timestamptz";
                case "type/datetime":
                    return "timestamp";
                case "type/date":
                    return "date";
                case "type/time":
                    return "time";
                case "type/text":
                    return "text";
                case "type/integer":
                    return "integer";
                case "type/biginteger":
                    return "bigint";
                case "type/decimal":
                    return "decimal";
                case "type/float":
                    return "float";
                case "type/blob":
                    return "blob";
                case "type/json":
                    return "json";
                default:
                    return effType.toLowerCase().replace("type/", "");
            }
        }
        
        // PRIORITY 3: base_type (fallback)
        if (baseType != null) {
            return baseType.toLowerCase().replace("type/", "");
        }
        
        return "unknown";
    }
    
    /**
     * Gets the column size (used in ResultSetMetaData).
     */
    public int getColumnSize() {
        // SPECIAL PRIORITY: UUID
        if (isUUID()) {
            return 36; // UUIDs always have 36 characters (standard format)
        }
        
        // For PostgreSQL, uses more specific information
        if (databaseType != null) {
            String dbType = databaseType.toLowerCase();
            switch (dbType) {
                case "uuid":
                    return 36; // UUIDs have 36 characters
                case "jsonb":
                case "json":
                case "text":
                    return 65535; // Large size for JSON/TEXT
                case "varchar":
                    return 255;
                case "timestamptz":
                case "timestamp":
                    return 26; // Timestamp with timezone
                case "int4":
                    return 10;
                case "int8":
                    return 19;
                case "numeric":
                    return 38;
                case "bool":
                    return 1;
                default:
                    break;
            }
        }
        
        // Fallback for original logic
        switch (getJdbcType()) {
            case Types.INTEGER:
                return 10;
            case Types.BIGINT:
                return 19;
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.FLOAT:
                return 38;
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.LONGVARCHAR:
                return 255; // Default value
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return 26;
            case Types.DATE:
                return 10;
            case Types.TIME:
                return 8;
            case Types.BOOLEAN:
                return 1;
            case Types.OTHER:
                // For OTHER types (JSON), uses a large size like in PostgreSQL
                return 2147483647; // Maximum PostgreSQL size for JSON
            default:
                return 255;
        }
    }
    
    /**
     * Gets the number of decimal digits (used in ResultSetMetaData).
     */
    public int getDecimalDigits() {
        switch (getJdbcType()) {
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.REAL:
                return 2; // Default for decimals
            default:
                return 0;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetabaseColumnInfo that = (MetabaseColumnInfo) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(displayName, that.displayName) &&
               Objects.equals(baseType, that.baseType) &&
               Objects.equals(semanticType, that.semanticType) &&
               Objects.equals(databaseType, that.databaseType) &&
               Objects.equals(effectiveType, that.effectiveType) &&
               Objects.equals(fieldId, that.fieldId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(name, displayName, baseType, semanticType, databaseType, effectiveType, fieldId);
    }
    
    @Override
    public String toString() {
        return String.format("MetabaseColumnInfo{name='%s', displayName='%s', baseType='%s', " +
                           "semanticType='%s', databaseType='%s', effectiveType='%s', fieldId='%s'}", 
                           name, displayName, baseType, semanticType, databaseType, effectiveType, fieldId);
    }
} 