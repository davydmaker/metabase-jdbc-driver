package com.davydmaker.metabase.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.sql.SQLException;

/**
 * Represents the result of a query executed in Metabase.
 * 
 */
public class MetabaseQueryResult {
    
    private final List<MetabaseColumnInfo> columns;
    private final List<List<Object>> rows;
    private final Integer runningTime; // execution time in ms
    private final Integer rowCount; // number of rows returned
    private final String status; // query status (completed, failed, etc)
    private final String resultsTimezone; // timezone of results
    private final Integer databaseId; // ID of the database in Metabase
    private int currentRowIndex = -1;
    
    // Execution information
    private final String startedAt; // timestamp of query start
    private final Boolean cached; // if the result came from cache
    private final String context; // execution context (ad-hoc, etc)
    private final Integer averageExecutionTime; // average execution time
    
    /**
     * Basic constructor (maintains compatibility).
     */
    public MetabaseQueryResult(List<MetabaseColumnInfo> columns, List<List<Object>> rows) {
        this(columns, rows, null, null, null, null, null, null, null, null, null);
    }
    
    /**
     * Constructor with all response information.
     */
    public MetabaseQueryResult(List<MetabaseColumnInfo> columns, List<List<Object>> rows, 
                              Integer runningTime, Integer rowCount, String status,
                              String resultsTimezone, Integer databaseId) {
        this(columns, rows, runningTime, rowCount, status, resultsTimezone, databaseId, null, null, null, null);
    }
    
    /**
     * Constructor with all response information including execution metadata.
     */
    public MetabaseQueryResult(List<MetabaseColumnInfo> columns, List<List<Object>> rows, 
                              Integer runningTime, Integer rowCount, String status,
                              String resultsTimezone, Integer databaseId, String startedAt,
                              Boolean cached, String context, Integer averageExecutionTime) {
        this.columns = columns;
        this.rows = rows;
        this.runningTime = runningTime;
        this.rowCount = rowCount;
        this.status = status;
        this.resultsTimezone = resultsTimezone;
        this.databaseId = databaseId;
        this.startedAt = startedAt;
        this.cached = cached;
        this.context = context;
        this.averageExecutionTime = averageExecutionTime;
    }
    
    /**
     * Gets the columns of the result.
     */
    public List<MetabaseColumnInfo> getColumns() {
        return columns;
    }
    
    /**
     * Gets the data rows.
     */
    public List<List<Object>> getRows() {
        return rows;
    }

    /**
     * Gets the number of columns.
     */
    public int getColumnCount() {
        return columns.size();
    }
    
    /**
     * Gets the query execution time in milliseconds.
     */
    public Integer getRunningTime() {
        return runningTime;
    }
    
    /**
     * Gets the number of rows returned.
     */
    public Integer getRowCount() {
        return rowCount;
    }

    /**
     * Gets information about a specific column.
     * 
     * @param columnIndex the column index (1-based)
     * @return column information
     */
    public MetabaseColumnInfo getColumnInfo(int columnIndex) {
        if (columnIndex < 1 || columnIndex > columns.size()) {
            throw new IndexOutOfBoundsException("Invalid column index: " + columnIndex);
        }
        return columns.get(columnIndex - 1);
    }

    /**
     * Gets information about a column by name.
     * 
     * @param columnName the column name
     * @return column information or null if not found
     */
    public MetabaseColumnInfo getColumnInfo(String columnName) {
        return columns.stream()
                     .filter(col -> col.getName().equalsIgnoreCase(columnName))
                     .findFirst()
                     .orElse(null);
    }

    /**
     * Gets the index of a column by name.
     * 
     * @param columnName the column name
     * @return column index (1-based) or -1 if not found
     */
    public int getColumnIndex(String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(columnName)) {
                return i + 1;
            }
        }
        return -1;
    }

    /**
     * Checks if there is a next row.
     */
    public boolean hasNext() {
        return currentRowIndex + 1 < rows.size();
    }
    
    /**
     * Moves to the next row.
     * 
     * @return true if there is a next row, false otherwise
     */
    public boolean next() {
        if (hasNext()) {
            currentRowIndex++;
            return true;
        }
        return false;
    }
    
    /**
     * Gets the current row.
     * 
     * @return list with the values of the current row
     */
    public List<Object> getCurrentRow() {
        if (currentRowIndex < 0 || currentRowIndex >= rows.size()) {
            return null;
        }
        return new ArrayList<>(rows.get(currentRowIndex));
    }
    
    /**
     * Gets the value of a column in the current row.
     * 
     * @param columnIndex the column index (1-based)
     * @return column value
     */
    public Object getValue(int columnIndex) {
        List<Object> currentRow = getCurrentRow();
        if (currentRow == null) {
            throw new IllegalStateException("No current row available");
        }
        
        if (columnIndex < 1 || columnIndex > currentRow.size()) {
            throw new IndexOutOfBoundsException("Invalid column index: " + columnIndex);
        }
        
        return currentRow.get(columnIndex - 1);
    }

    /**
     * Gets the value of a column in the current row by name.
     * 
     * @param columnName the column name
     * @return column value
     */
    public Object getValue(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column not found: " + columnName);
        }
        return getValue(columnIndex);
    }
    
    /**
     * Moves the cursor to before the first row.
     */
    public void beforeFirst() {
        currentRowIndex = -1;
    }
    
    /**
     * Moves the cursor to the first row.
     * 
     * @return true if there is a first row, false otherwise
     */
    public boolean first() {
        if (!rows.isEmpty()) {
            currentRowIndex = 0;
            return true;
        }
        return false;
    }

    /**
     * Moves the cursor to the last row.
     * 
     * @return true if there is a last row, false otherwise
     */
    public boolean last() {
        if (!rows.isEmpty()) {
            currentRowIndex = rows.size() - 1;
            return true;
        }
        return false;
    }
    
    /**
     * Moves the cursor to after the last row.
     */
    public void afterLast() {
        currentRowIndex = rows.size();
    }
    
    /**
     * Checks if the cursor is before the first row.
     */
    public boolean isBeforeFirst() {
        return currentRowIndex == -1;
    }
    
    /**
     * Checks if the cursor is on the first row.
     */
    public boolean isFirst() {
        return currentRowIndex == 0 && !rows.isEmpty();
    }
    
    /**
     * Checks if the cursor is on the last row.
     */
    public boolean isLast() {
        return currentRowIndex == rows.size() - 1 && !rows.isEmpty();
    }
    
    /**
     * Checks if the cursor is after the last row.
     */
    public boolean isAfterLast() {
        return currentRowIndex >= rows.size();
    }

    /**
     * Gets the current cursor position.
     * 
     * @return cursor position (1-based) or 0 if before the first row
     */
    public int getRow() {
        if (currentRowIndex < 0) {
            return 0;
        }
        return currentRowIndex + 1; // JDBC uses 1-based index
    }
    
    /**
     * Checks if the result is empty.
     */
    public boolean isEmpty() {
        return rows.isEmpty();
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetabaseQueryResult that = (MetabaseQueryResult) o;
        return Objects.equals(columns, that.columns) &&
               Objects.equals(rows, that.rows);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(columns, rows);
    }
    
    @Override
    public String toString() {
        return String.format("MetabaseQueryResult{columns=%d, rows=%d, currentRow=%d}", 
                           columns.size(), rows.size(), currentRowIndex);
    }
    
    /**
     * Gets the query status.
     */
    public String getStatus() {
        return status;
    }
    
    /**
     * Gets the timezone of results.
     */
    public String getResultsTimezone() {
        return resultsTimezone;
    }
    
    /**
     * Gets the ID of the database in Metabase.
     */
    public Integer getDatabaseId() {
        return databaseId;
    }
    
    /**
     * Gets the query start timestamp.
     */
    public String getStartedAt() {
        return startedAt;
    }
    
    /**
     * Checks if the result came from cache.
     */
    public Boolean isCached() {
        return cached;
    }
    
    /**
     * Gets the execution context.
     */
    public String getContext() {
        return context;
    }
    
    /**
     * Gets the average execution time.
     */
    public Integer getAverageExecutionTime() {
        return averageExecutionTime;
    }
    
    /**
     * Returns query performance information.
     */
    public String getPerformanceInfo() {
        StringBuilder info = new StringBuilder();
        
        if (runningTime != null) {
            info.append("executed in ").append(runningTime).append("ms");
        }
        
        if (rowCount != null) {
            if (info.length() > 0) info.append(", ");
            info.append(rowCount).append(" row(s)");
        }
        
        if (cached != null && cached) {
            if (info.length() > 0) info.append(", ");
            info.append("from cache");
        }
        
        if (context != null) {
            if (info.length() > 0) info.append(", ");
            info.append("context: ").append(context);
        }
        
        if (averageExecutionTime != null) {
            if (info.length() > 0) info.append(", ");
            info.append("average: ").append(averageExecutionTime).append("ms");
        }
        
        return info.length() > 0 ? info.toString() : "Performance information not available";
    }
    
    /**
     * Creates a MetabaseQueryResult from JSON response.
     * 
     * @param responseJson JSON response from Metabase API
     * @param objectMapper Jackson ObjectMapper for JSON parsing
     * @return Parsed query result
     * @throws SQLException If parsing fails
     */
    public static MetabaseQueryResult fromJson(com.fasterxml.jackson.databind.JsonNode responseJson, 
                                             com.fasterxml.jackson.databind.ObjectMapper objectMapper) throws SQLException {
        try {
            if (responseJson.has("error") && responseJson.get("error") != null && !responseJson.get("error").isNull()) {
                throw new SQLException(responseJson.get("error").asText());
            }

            Integer runningTime = responseJson.has("running_time") ? responseJson.get("running_time").asInt() : null;
            Integer rowCount = responseJson.has("row_count") ? responseJson.get("row_count").asInt() : null;
            String status = responseJson.has("status") ? responseJson.get("status").asText() : null;
            Integer databaseId = responseJson.has("database_id") ? responseJson.get("database_id").asInt() : null;
            
            String startedAt = responseJson.has("started_at") ? responseJson.get("started_at").asText() : null;
            Boolean cached = responseJson.has("cached") && !responseJson.get("cached").isNull() ? 
                           responseJson.get("cached").asBoolean() : null;
            String context = responseJson.has("context") ? responseJson.get("context").asText() : null;
            Integer averageExecutionTime = responseJson.has("average_execution_time") && 
                                         !responseJson.get("average_execution_time").isNull() ? 
                                         responseJson.get("average_execution_time").asInt() : null;
            
            com.fasterxml.jackson.databind.JsonNode dataNode = responseJson.get("data");
            if (dataNode == null) {
                throw new SQLException("API response does not contain data");
            }
            
            String resultsTimezone = dataNode.has("results_timezone") ? 
                                   dataNode.get("results_timezone").asText() : null;
            
            com.fasterxml.jackson.databind.JsonNode colsNode = dataNode.get("cols");
            List<MetabaseColumnInfo> columns = new ArrayList<>();
            
            if (colsNode != null && colsNode.isArray()) {
                for (int i = 0; i < colsNode.size(); i++) {
                    com.fasterxml.jackson.databind.JsonNode colNode = colsNode.get(i);
                    String name = colNode.get("name").asText();
                    String displayName = colNode.get("display_name").asText();
                    String baseType = colNode.get("base_type").asText();
                    String semanticType = colNode.has("semantic_type") && !colNode.get("semantic_type").isNull() 
                                        ? colNode.get("semantic_type").asText() : null;
                    
                    String databaseType = colNode.has("database_type") ? colNode.get("database_type").asText() : null;
                    String effectiveType = colNode.has("effective_type") ? colNode.get("effective_type").asText() : null;
                    
                    MetabaseColumnInfo column = new MetabaseColumnInfo(name, displayName, baseType, 
                                                                     semanticType, databaseType, effectiveType);
                    columns.add(column);
                }
            } else {
                throw new SQLException("Column metadata not found");
            }
            
            com.fasterxml.jackson.databind.JsonNode rowsNode = dataNode.get("rows");
            List<List<Object>> rows = new ArrayList<>();
            
            if (rowsNode != null && rowsNode.isArray()) {
                for (com.fasterxml.jackson.databind.JsonNode rowNode : rowsNode) {
                    List<Object> row = new ArrayList<>();
                    if (rowNode.isArray()) {
                        for (int i = 0; i < rowNode.size(); i++) {
                            com.fasterxml.jackson.databind.JsonNode cellNode = rowNode.get(i);
                            
                            MetabaseColumnInfo columnInfo = i < columns.size() ? columns.get(i) : null;
                            Object value = convertJsonValue(cellNode, columnInfo);
                            row.add(value);
                        }
                    }
                    rows.add(row);
                }
            } else {
                throw new SQLException("Data rows not found");
            }
            
            return new MetabaseQueryResult(columns, rows, runningTime, 
                                         rowCount, status, resultsTimezone, databaseId,
                                         startedAt, cached, context, averageExecutionTime);
            
        } catch (Exception e) {
            throw new SQLException("\n" + e.getMessage(), e);
        }
    }
    
    /**
     * Converts JSON node to appropriate Java type with column type hints.
     */
    private static Object convertJsonValue(com.fasterxml.jackson.databind.JsonNode node, MetabaseColumnInfo columnInfo) {
        if (node == null || node.isNull()) {
            return null;
        }
        
        if (columnInfo != null && columnInfo.getDatabaseType() != null) {
            String dbType = columnInfo.getDatabaseType().toLowerCase();
            if ("timestamptz".equals(dbType) && node.isTextual()) {
                return convertTimestampWithTimezone(node.textValue());
            }
        }
        
        return convertJsonValue(node);
    }
    
    /**
     * Converts JSON node to appropriate Java type.
     */
    private static Object convertJsonValue(com.fasterxml.jackson.databind.JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        } else if (node.isBoolean()) {
            return node.booleanValue();
        } else if (node.isInt()) {
            return node.intValue();
        } else if (node.isLong()) {
            return node.longValue();
        } else if (node.isDouble()) {
            return node.doubleValue();
        } else if (node.isFloat()) {
            return node.floatValue();
        } else if (node.isTextual()) {
            return node.textValue();
        } else if (node.isObject() || node.isArray()) {
            return node.toString();
        } else {
            return node.toString();
        }
    }
    
    /**
     * Converts UTC timestamp string to SQL Timestamp.
     */
    private static Object convertTimestampWithTimezone(String utcTimestamp) {
        try {
            java.time.Instant instant;
            
            if (utcTimestamp.endsWith("Z")) {
                instant = java.time.Instant.parse(utcTimestamp);
            } else if (utcTimestamp.contains("+") || utcTimestamp.contains("-")) {
                instant = java.time.OffsetDateTime.parse(utcTimestamp).toInstant();
            } else {
                instant = java.time.LocalDateTime.parse(utcTimestamp).atZone(java.time.ZoneOffset.UTC).toInstant();
            }
            
            return java.sql.Timestamp.from(instant);
            
        } catch (Exception e) {
            return utcTimestamp;
        }
    }
} 