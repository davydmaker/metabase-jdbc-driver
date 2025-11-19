package com.davydmaker.metabase.jdbc;

import com.davydmaker.metabase.api.MetabaseColumnInfo;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * ResultSetMetaData implementation for the Metabase JDBC driver.
 * 
 * This class provides metadata about the columns of a ResultSet,
 * including names, types and column properties.
 * 
 */
public class MetabaseResultSetMetaData implements ResultSetMetaData {
    private final List<MetabaseColumnInfo> columns;
    
    /**
     * Creates a new MetabaseResultSetMetaData instance.
     * 
     * @param columns the list of column information
     */
    public MetabaseResultSetMetaData(List<MetabaseColumnInfo> columns) {
        this.columns = columns;
    }
    
    @Override
    public int getColumnCount() throws SQLException {
        return columns.size();
    }
    
    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        validateColumnIndex(column);
        return false;
    }
    
    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        validateColumnIndex(column);
        return true;
    }
    
    @Override
    public boolean isSearchable(int column) throws SQLException {
        validateColumnIndex(column);
        return true;
    }
    
    @Override
    public boolean isCurrency(int column) throws SQLException {
        validateColumnIndex(column);
        MetabaseColumnInfo columnInfo = columns.get(column - 1);
        String semanticType = columnInfo.getSemanticType();
        return semanticType != null && semanticType.toLowerCase().contains("currency");
    }
    
    @Override
    public int isNullable(int column) throws SQLException {
        validateColumnIndex(column);
        return columnNullable;
    }
    
    @Override
    public boolean isSigned(int column) throws SQLException {
        validateColumnIndex(column);
        int type = getColumnType(column);
        return type == Types.INTEGER || type == Types.BIGINT || type == Types.SMALLINT ||
               type == Types.TINYINT || type == Types.DECIMAL || type == Types.DOUBLE ||
               type == Types.FLOAT || type == Types.REAL;
    }
    
    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        validateColumnIndex(column);
        MetabaseColumnInfo columnInfo = columns.get(column - 1);
        return columnInfo.getColumnSize();
    }
    
    @Override
    public String getColumnLabel(int column) throws SQLException {
        validateColumnIndex(column);
        MetabaseColumnInfo columnInfo = columns.get(column - 1);
        String displayName = columnInfo.getDisplayName();
        return displayName != null ? displayName : columnInfo.getName();
    }
    
    @Override
    public String getColumnName(int column) throws SQLException {
        validateColumnIndex(column);
        MetabaseColumnInfo columnInfo = columns.get(column - 1);
        return columnInfo.getName();
    }
    
    @Override
    public String getSchemaName(int column) throws SQLException {
        validateColumnIndex(column);
        return "";
    }
    
    @Override
    public int getPrecision(int column) throws SQLException {
        validateColumnIndex(column);
        MetabaseColumnInfo columnInfo = columns.get(column - 1);
        
        int type = columnInfo.getJdbcType();
        switch (type) {
            case Types.DECIMAL:
            case Types.NUMERIC:
                return 38;
            case Types.DOUBLE:
                return 15;
            case Types.FLOAT:
            case Types.REAL:
                return 7;
            case Types.INTEGER:
                return 10;
            case Types.BIGINT:
                return 19;
            case Types.SMALLINT:
                return 5;
            case Types.TINYINT:
                return 3;
            case Types.OTHER:
                return 2147483647;
            default:
                return columnInfo.getColumnSize();
        }
    }
    
    @Override
    public int getScale(int column) throws SQLException {
        validateColumnIndex(column);
        MetabaseColumnInfo columnInfo = columns.get(column - 1);
        return columnInfo.getDecimalDigits();
    }
    
    @Override
    public String getTableName(int column) throws SQLException {
        validateColumnIndex(column);
        return "";
    }
    
    @Override
    public String getCatalogName(int column) throws SQLException {
        validateColumnIndex(column);
        return "";
    }
    
    @Override
    public int getColumnType(int column) throws SQLException {
        validateColumnIndex(column);
        MetabaseColumnInfo columnInfo = columns.get(column - 1);
        return columnInfo.getJdbcType();
    }
    
    @Override
    public String getColumnTypeName(int column) throws SQLException {
        validateColumnIndex(column);
        MetabaseColumnInfo columnInfo = columns.get(column - 1);
        
        return columnInfo.getDisplayTypeName();
    }
    
    @Override
    public boolean isReadOnly(int column) throws SQLException {
        validateColumnIndex(column);
        return true;
    }
    
    @Override
    public boolean isWritable(int column) throws SQLException {
        validateColumnIndex(column);
        return false;
    }
    
    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        validateColumnIndex(column);
        return false;
    }
    
    @Override
    public String getColumnClassName(int column) throws SQLException {
        validateColumnIndex(column);
        MetabaseColumnInfo columnInfo = columns.get(column - 1);
        return columnInfo.getJavaTypeName();
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
     * Validates if the column index is valid.
     * 
     * @param column the column index (1-based)
     * @throws SQLException if the index is invalid
     */
    private void validateColumnIndex(int column) throws SQLException {
        if (column < 1 || column > columns.size()) {
            throw new SQLException("Invalid column index: " + column + 
                                 " (must be between 1 and " + columns.size() + ")");
        }
    }
} 