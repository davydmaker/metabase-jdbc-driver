package com.davydmaker.metabase.jdbc;

import com.davydmaker.metabase.api.MetabaseApiClient;
import com.davydmaker.metabase.api.MetabaseDatabase;
import com.davydmaker.metabase.api.MetabaseDriverInfo;
import com.davydmaker.metabase.api.MetabaseTableInfo;
import com.davydmaker.metabase.api.MetabaseColumnInfo;
import com.davydmaker.metabase.api.MetabaseQueryResult;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DatabaseMetaData implementation for the Metabase JDBC driver.
 * 
 * Provides metadata about the database connected through Metabase,
 * including information about tables, columns, indexes and driver capabilities.
 * 
 */
public class MetabaseDatabaseMetaData implements DatabaseMetaData {
      
    private final MetabaseConnection connection;
    private final MetabaseApiClient apiClient;
    private final String databaseName;
    
    /**
     * Creates a new MetabaseDatabaseMetaData instance.
     * 
     * @param connection the Metabase connection
     * @param apiClient the Metabase API client
     * @param databaseName the database name
     */
    public MetabaseDatabaseMetaData(MetabaseConnection connection, MetabaseApiClient apiClient, String databaseName) {
        this.connection = connection;
        this.apiClient = apiClient;
        this.databaseName = databaseName;
    }
    
    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }
    
    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }
    
    @Override
    public String getURL() throws SQLException {
        return connection.getUrl();
    }
    
    @Override
    public String getUserName() throws SQLException {
        return "";
    }
    
    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }
    
    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }
    
    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }
    
    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }
    
    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }
    
    @Override
    public String getDatabaseProductName() throws SQLException {
        MetabaseDatabase db = apiClient.getDatabaseByName(databaseName);
        if (db != null) {
            return "Metabase (" + db.getEngine() + ")";
        }
        return "Metabase";
    }
    
    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return "via Metabase API";
    }
    
    @Override
    public String getDriverName() throws SQLException {
        return MetabaseDriverInfo.DRIVER_NAME;
    }
    
    @Override
    public String getDriverVersion() throws SQLException {
        return MetabaseDriverInfo.DRIVER_VERSION;
    }
    
    @Override
    public int getDriverMajorVersion() {
        return MetabaseDriver.MAJOR_VERSION;
    }
    
    @Override
    public int getDriverMinorVersion() {
        return MetabaseDriver.MINOR_VERSION;
    }
    
    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }
    
    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }
    
    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }
    
    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }
    
    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }
    
    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }
    
    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }
    
    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }
    
    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }
    
    @Override
    public String getSQLKeywords() throws SQLException {
        return "";
    }
    
    @Override
    public String getNumericFunctions() throws SQLException {
        return "ABS,CEIL,FLOOR,ROUND,SQRT,MIN,MAX,AVG,SUM,COUNT";
    }
    
    @Override
    public String getStringFunctions() throws SQLException {
        return "CONCAT,LENGTH,LOWER,UPPER,SUBSTRING,TRIM,LTRIM,RTRIM";
    }
    
    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }
    
    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "NOW,CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,YEAR,MONTH,DAY";
    }
    
    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }
    
    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }
    
    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }
    
    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }
    
    @Override
    public String getSchemaTerm() throws SQLException {
        return "schema";
    }
    
    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }
    
    @Override
    public String getCatalogTerm() throws SQLException {
        return "catalog";
    }
    
    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }
    
    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }
    
    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsUnion() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }
    
    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }
    
    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }
    
    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }
    
    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 128;
    }
    
    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }
    
    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }
    
    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }
    
    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }
    
    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }
    
    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }
    
    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }
    
    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }
    
    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 128;
    }
    
    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }
    
    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 128;
    }
    
    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }
    
    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }
    
    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }
    
    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }
    
    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 128;
    }
    
    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }
    
    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 128;
    }
    
    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }
    
    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return level == Connection.TRANSACTION_NONE;
    }
    
    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }
    
    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }
    
    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }
    
    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY && 
               concurrency == ResultSet.CONCUR_READ_ONLY;
    }
    
    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }
    
    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }
    
    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }
    
    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }
    
    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }
    
    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }
    
    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }
    
    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }
    
    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }
    
    @Override
    public Connection getConnection() throws SQLException {
        return connection;
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
    
    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return createEmptyResultSet();
    }
    
    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return createEmptyResultSet();
    }
    
    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        List<List<Object>> rows = new ArrayList<>();
        
        try {
            List<String> schemas;
            
            if (schemaPattern == null || schemaPattern.isEmpty() || "%".equals(schemaPattern)) {
                schemas = apiClient.getSchemas();
            } else {
                schemas = List.of(schemaPattern);
            }
            
            for (String schema : schemas) {
                try {
                    addTablesFromSchema(schema, tableNamePattern, types, rows);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            throw new SQLException("Error retrieving tables: " + e.getMessage(), e);
        }
        
        // Create columns for the result set
        List<MetabaseColumnInfo> columns = new ArrayList<>();
        columns.add(new MetabaseColumnInfo("TABLE_CAT", "TABLE_CAT", "type/Text", null));
        columns.add(new MetabaseColumnInfo("TABLE_SCHEM", "TABLE_SCHEM", "type/Text", null));
        columns.add(new MetabaseColumnInfo("TABLE_NAME", "TABLE_NAME", "type/Text", null));
        columns.add(new MetabaseColumnInfo("TABLE_TYPE", "TABLE_TYPE", "type/Text", null));
        columns.add(new MetabaseColumnInfo("REMARKS", "REMARKS", "type/Text", null));
        columns.add(new MetabaseColumnInfo("TYPE_CAT", "TYPE_CAT", "type/Text", null));
        columns.add(new MetabaseColumnInfo("TYPE_SCHEM", "TYPE_SCHEM", "type/Text", null));
        columns.add(new MetabaseColumnInfo("TYPE_NAME", "TYPE_NAME", "type/Text", null));
        columns.add(new MetabaseColumnInfo("SELF_REFERENCING_COL_NAME", "SELF_REFERENCING_COL_NAME", "type/Text", null));
        columns.add(new MetabaseColumnInfo("REF_GENERATION", "REF_GENERATION", "type/Text", null));
        
        MetabaseQueryResult result = new MetabaseQueryResult(columns, rows);
        return new MetabaseResultSet(null, result);
    }
    
    /**
     * Adds tables from a specific schema to the result.
     */
    private void addTablesFromSchema(String schemaName, String tableNamePattern, String[] types, 
                                   List<List<Object>> rows) throws SQLException {
        try {
            List<MetabaseTableInfo> tables = apiClient.getTables(schemaName);
            
            for (MetabaseTableInfo table : tables) {
                if (tableNamePattern != null && !tableNamePattern.isEmpty() && !"%".equals(tableNamePattern)) {
                    String pattern = tableNamePattern.replace("%", ".*").replace("_", ".");
                    if (!table.getName().matches(pattern)) {
                        continue;
                    }
                }
                
                String tableType = "TABLE";
                
                if (types != null && types.length > 0) {
                    boolean typeMatches = false;
                    for (String type : types) {
                        if (tableType.equals(type)) {
                            typeMatches = true;
                            break;
                        }
                    }
                    if (!typeMatches) {
                        continue;
                    }
                }
                
                List<Object> row = new ArrayList<>();
                row.add(null);
                row.add(table.getSchema());
                row.add(table.getName());
                row.add(tableType);
                row.add(table.getDescription());
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                
                rows.add(row);
            }
            
        } catch (Exception e) {
            throw new SQLException("Error loading tables from schema: " + e.getMessage(), e);
        }
    }
    
    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }
    
    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        try {
            List<String> schemas = apiClient.getSchemas();
            
            if (schemaPattern != null && !schemaPattern.isEmpty() && !"%".equals(schemaPattern)) {
                String pattern = schemaPattern.replace("%", ".*").replace("_", ".");
                schemas = schemas.stream()
                    .filter(schema -> schema.matches(pattern))
                    .collect(Collectors.toList());
            }
            
            List<List<Object>> rows = new ArrayList<>();
            for (String schema : schemas) {
                List<Object> row = new ArrayList<>();
                row.add(schema);
                row.add(null);
                rows.add(row);
            }
            
            List<MetabaseColumnInfo> columns = new ArrayList<>();
            columns.add(new MetabaseColumnInfo("TABLE_SCHEM", "TABLE_SCHEM", "type/Text", null));
            columns.add(new MetabaseColumnInfo("TABLE_CATALOG", "TABLE_CATALOG", "type/Text", null));
            
            MetabaseQueryResult result = new MetabaseQueryResult(columns, rows);
            return new MetabaseResultSet(null, result);
            
        } catch (Exception e) {
            throw new SQLException("Error retrieving schemas: " + e.getMessage(), e);
        }
    }
    
    @Override
    public ResultSet getCatalogs() throws SQLException {
        return createEmptyResultSet();
    }
    
    @Override
    public ResultSet getTableTypes() throws SQLException {
        List<List<Object>> rows = new ArrayList<>();
        
        rows.add(List.of("TABLE"));
        rows.add(List.of("VIEW"));
        rows.add(List.of("SYSTEM TABLE"));
        
        List<MetabaseColumnInfo> columns = new ArrayList<>();
        columns.add(new MetabaseColumnInfo("TABLE_TYPE", "TABLE_TYPE", "type/Text", null));
        
        MetabaseQueryResult result = new MetabaseQueryResult(columns, rows);
        return new MetabaseResultSet(null, result);
    }
    
    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        try {
            List<List<Object>> rows = new ArrayList<>();
            
            if (schemaPattern != null && !schemaPattern.isEmpty() && !"%".equals(schemaPattern) &&
                tableNamePattern != null && !tableNamePattern.isEmpty() && !"%".equals(tableNamePattern)) {
                
                List<MetabaseTableInfo> matchingTables = findSpecificTables(schemaPattern, tableNamePattern);
                for (MetabaseTableInfo table : matchingTables) {
                    addColumnsFromTable(table, columnNamePattern, rows);
                }
                
            } else if (schemaPattern != null && !schemaPattern.isEmpty() && !"%".equals(schemaPattern)) {
                
                String pattern = schemaPattern.replace("%", ".*").replace("_", ".");
                List<String> schemas = apiClient.getSchemas().stream()
                    .filter(schema -> schema.matches(pattern))
                    .collect(Collectors.toList());
                
                for (String schema : schemas.subList(0, Math.min(schemas.size(), 1))) {
                    List<MetabaseTableInfo> tables = apiClient.getTables(schema);
                    for (MetabaseTableInfo table : tables.subList(0, Math.min(tables.size(), 3))) {
                        if (matchesTablePattern(table.getName(), tableNamePattern)) {
                            addColumnsFromTable(table, columnNamePattern, rows);
                        }
                    }
                }
                
            } else {
                
                List<String> schemas = apiClient.getSchemas();
                for (String schema : schemas.subList(0, Math.min(schemas.size(), 1))) {
                    List<MetabaseTableInfo> tables = apiClient.getTables(schema);
                    for (MetabaseTableInfo table : tables.subList(0, Math.min(tables.size(), 2))) {
                        if (matchesTablePattern(table.getName(), tableNamePattern)) {
                            addColumnsFromTable(table, columnNamePattern, rows);
                        }
                    }
                }
            }
            
            List<MetabaseColumnInfo> columns = createColumnInfoResultSet();
            
            MetabaseQueryResult result = 
                new MetabaseQueryResult(columns, rows);
            
            return new MetabaseResultSet(null, result);
            
        } catch (Exception e) {
            throw new SQLException("Error retrieving columns: " + e.getMessage(), e);
        }
    }
    
    /**
     * Finds specific tables based on schema and table patterns.
     */
    private List<MetabaseTableInfo> findSpecificTables(String schemaPattern, String tableNamePattern) throws SQLException {
        List<MetabaseTableInfo> matchingTables = new ArrayList<>();
        
        String schemaRegex = schemaPattern.replace("%", ".*").replace("_", ".");
        String tableRegex = tableNamePattern.replace("%", ".*").replace("_", ".");
        
        List<String> schemas = apiClient.getSchemas().stream()
            .filter(schema -> schema.matches(schemaRegex))
            .collect(Collectors.toList());
        
        for (String schema : schemas) {
            List<MetabaseTableInfo> tables = apiClient.getTables(schema);
            for (MetabaseTableInfo table : tables) {
                if (table.getName().matches(tableRegex)) {
                    matchingTables.add(table);
                }
            }
        }
        
        return matchingTables;
    }
    
    /**
     * Checks if a table name matches the pattern.
     */
    private boolean matchesTablePattern(String tableName, String tableNamePattern) {
        if (tableNamePattern == null || tableNamePattern.isEmpty() || "%".equals(tableNamePattern)) {
            return true;
        }
        String pattern = tableNamePattern.replace("%", ".*").replace("_", ".");
        return tableName.matches(pattern);
    }
    
    /**
     * Adds columns from a specific table to the result.
     */
    private void addColumnsFromTable(MetabaseTableInfo table, 
                                   String columnNamePattern, List<List<Object>> rows) throws SQLException {
        try {
            List<MetabaseColumnInfo> columns = apiClient.getTableColumns(table.getId());
            
            int ordinalPosition = 1;
            for (MetabaseColumnInfo column : columns) {
                if (columnNamePattern != null && !columnNamePattern.isEmpty() && !"%".equals(columnNamePattern)) {
                    String pattern = columnNamePattern.replace("%", ".*").replace("_", ".");
                    if (!column.getName().matches(pattern)) {
                        continue;
                    }
                }
                
                String typeName = column.getDisplayTypeName();
                int jdbcType = column.getJdbcType();
                
                List<Object> row = new ArrayList<>();
                row.add(null);
                row.add(table.getSchema());
                row.add(table.getName());
                row.add(column.getName());
                row.add(jdbcType);
                row.add(typeName);
                row.add(column.getColumnSize());
                row.add(null);
                row.add(column.getDecimalDigits());
                row.add(10);
                row.add(DatabaseMetaData.columnNullable);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add("NO");
                row.add(null);
                row.add(null);
                row.add(null);
                row.add("NO");
                row.add("NO");
                
                rows.add(row);
                ordinalPosition++;
            }
        } catch (Exception e) {
        }
    }
    
    /**
     * Creates the column structure for the column information ResultSet.
     */
    private List<MetabaseColumnInfo> createColumnInfoResultSet() {
        List<MetabaseColumnInfo> columns = new ArrayList<>();
        
        columns.add(new MetabaseColumnInfo("TABLE_CAT", "TABLE_CAT", "type/Text", null));
        columns.add(new MetabaseColumnInfo("TABLE_SCHEM", "TABLE_SCHEM", "type/Text", null));
        columns.add(new MetabaseColumnInfo("TABLE_NAME", "TABLE_NAME", "type/Text", null));
        columns.add(new MetabaseColumnInfo("COLUMN_NAME", "COLUMN_NAME", "type/Text", null));
        columns.add(new MetabaseColumnInfo("DATA_TYPE", "DATA_TYPE", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("TYPE_NAME", "TYPE_NAME", "type/Text", null));
        columns.add(new MetabaseColumnInfo("COLUMN_SIZE", "COLUMN_SIZE", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("BUFFER_LENGTH", "BUFFER_LENGTH", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("DECIMAL_DIGITS", "DECIMAL_DIGITS", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("NUM_PREC_RADIX", "NUM_PREC_RADIX", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("NULLABLE", "NULLABLE", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("REMARKS", "REMARKS", "type/Text", null));
        columns.add(new MetabaseColumnInfo("COLUMN_DEF", "COLUMN_DEF", "type/Text", null));
        columns.add(new MetabaseColumnInfo("SQL_DATA_TYPE", "SQL_DATA_TYPE", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("SQL_DATETIME_SUB", "SQL_DATETIME_SUB", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("CHAR_OCTET_LENGTH", "CHAR_OCTET_LENGTH", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("ORDINAL_POSITION", "ORDINAL_POSITION", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("IS_NULLABLE", "IS_NULLABLE", "type/Text", null));
        columns.add(new MetabaseColumnInfo("SCOPE_CATALOG", "SCOPE_CATALOG", "type/Text", null));
        columns.add(new MetabaseColumnInfo("SCOPE_SCHEMA", "SCOPE_SCHEMA", "type/Text", null));
        columns.add(new MetabaseColumnInfo("SCOPE_TABLE", "SCOPE_TABLE", "type/Text", null));
        columns.add(new MetabaseColumnInfo("SOURCE_DATA_TYPE", "SOURCE_DATA_TYPE", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("IS_AUTOINCREMENT", "IS_AUTOINCREMENT", "type/Text", null));
        columns.add(new MetabaseColumnInfo("IS_GENERATEDCOLUMN", "IS_GENERATEDCOLUMN", "type/Text", null));
        
        return columns;
    }
    
    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return createEmptyResultSet();
    }
    
    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return createEmptyResultSet();
    }
    
    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return createEmptyResultSet();
    }
    
    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return createEmptyResultSet();
    }
    
    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        List<List<Object>> rows = new ArrayList<>();
        
        if (table != null && !table.isEmpty()) {
            try {
                List<MetabaseTableInfo> tables = apiClient.getTables(schema);
                
                for (MetabaseTableInfo tableInfo : tables) {
                    if (table.equals(tableInfo.getName()) || table.equals("%")) {
                        List<MetabaseColumnInfo> columns = apiClient.getTableColumns(tableInfo.getId());
                        
                        int keySeq = 1;
                        for (MetabaseColumnInfo column : columns) {
                            if (column.isPrimaryKey()) {
                                List<Object> row = new ArrayList<>();
                                row.add(null);
                                row.add(tableInfo.getSchema());
                                row.add(tableInfo.getName());
                                row.add(column.getName());
                                row.add(keySeq);
                                row.add("PRIMARY");
                                
                                rows.add(row);
                                keySeq++;
                            }
                        }
                        
                        if (!table.equals("%")) {
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                // Silently ignore errors
            }
        }
        
        List<MetabaseColumnInfo> columns = createPrimaryKeysResultSet();
        
        MetabaseQueryResult result = 
            new MetabaseQueryResult(columns, rows);
        
        return new MetabaseResultSet(null, result);
    }
    
    /**
     * Creates the column structure for the primary keys ResultSet.
     */
    private List<MetabaseColumnInfo> createPrimaryKeysResultSet() {
        List<MetabaseColumnInfo> columns = new ArrayList<>();
        
        columns.add(new MetabaseColumnInfo("TABLE_CAT", "TABLE_CAT", "type/Text", null));
        columns.add(new MetabaseColumnInfo("TABLE_SCHEM", "TABLE_SCHEM", "type/Text", null));
        columns.add(new MetabaseColumnInfo("TABLE_NAME", "TABLE_NAME", "type/Text", null));
        columns.add(new MetabaseColumnInfo("COLUMN_NAME", "COLUMN_NAME", "type/Text", null));
        columns.add(new MetabaseColumnInfo("KEY_SEQ", "KEY_SEQ", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("PK_NAME", "PK_NAME", "type/Text", null));
        
        return columns; 
    }
    
    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return createEmptyResultSet();
    }
    
    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return createEmptyResultSet();
    }
    
    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return createEmptyResultSet();
    }
    
    @Override
    public ResultSet getTypeInfo() throws SQLException {
        try {
            List<List<Object>> rows = new ArrayList<>();
            
            addStandardSqlTypes(rows);
            
            List<MetabaseColumnInfo> columns = createTypeInfoColumns();
            
            MetabaseQueryResult result = 
                new MetabaseQueryResult(columns, rows);
            
            return new MetabaseResultSet(null, result);
            
        } catch (Exception e) {
            return createEmptyResultSet();
        }
    }
    
    /**
     * Adds standard SQL types to the getTypeInfo() result.
     */
    private void addStandardSqlTypes(List<List<Object>> rows) {
        addTypeInfoRow(rows, "VARCHAR", Types.VARCHAR, 65535, "'", "'", "length", 1, true, 3, false, false, false, "VARCHAR", (short) 0, (short) 0, Types.VARCHAR, 0, 10);
        addTypeInfoRow(rows, "INTEGER", Types.INTEGER, 10, null, null, null, 1, false, 2, false, false, true, "INTEGER", (short) 0, (short) 0, Types.INTEGER, 0, 10);
        addTypeInfoRow(rows, "BIGINT", Types.BIGINT, 19, null, null, null, 1, false, 2, false, false, true, "BIGINT", (short) 0, (short) 0, Types.BIGINT, 0, 10);
        addTypeInfoRow(rows, "BOOLEAN", Types.BOOLEAN, 1, null, null, null, 1, false, 2, false, false, false, "BOOLEAN", (short) 0, (short) 0, Types.BOOLEAN, 0, 2);
        addTypeInfoRow(rows, "TIMESTAMP", Types.TIMESTAMP, 26, "'", "'", null, 1, false, 3, false, false, false, "TIMESTAMP", (short) 0, (short) 6, Types.TIMESTAMP, 0, 10);
        addTypeInfoRow(rows, "DECIMAL", Types.DECIMAL, 38, null, null, "precision,scale", 1, false, 2, false, false, true, "DECIMAL", (short) 0, (short) 38, Types.DECIMAL, 0, 10);
        
        addTypeInfoRow(rows, "UUID", Types.OTHER, 36, "'", "'", null, 1, false, 3, false, false, false, "UUID", (short) 0, (short) 0, Types.OTHER, 0, 10);
        
        addTypeInfoRow(rows, "JSONB", Types.OTHER, 2147483647, "'", "'", null, 1, true, 3, false, false, false, "JSONB", (short) 0, (short) 0, Types.OTHER, 0, 10);
        addTypeInfoRow(rows, "JSON", Types.OTHER, 2147483647, "'", "'", null, 1, true, 3, false, false, false, "JSON", (short) 0, (short) 0, Types.OTHER, 0, 10);
    }
    
    /**
     * Adds a row of type information.
     */
    private void addTypeInfoRow(List<List<Object>> rows, String typeName, int dataType, int precision, 
                               String literalPrefix, String literalSuffix, String createParams,
                               int nullable, boolean caseSensitive, int searchable, 
                               boolean unsignedAttribute, boolean fixedPrecScale, boolean autoIncrement,
                               String localTypeName, short minimumScale, short maximumScale,
                               int sqlDataType, int sqlDatetimeSub, int numPrecRadix) {
        
        List<Object> row = new ArrayList<>();
        row.add(typeName);
        row.add(dataType);
        row.add(precision);
        row.add(literalPrefix);
        row.add(literalSuffix);
        row.add(createParams);
        row.add(nullable);
        row.add(caseSensitive);
        row.add(searchable);
        row.add(unsignedAttribute);
        row.add(fixedPrecScale);
        row.add(autoIncrement);
        row.add(localTypeName);
        row.add(minimumScale);
        row.add(maximumScale);
        row.add(sqlDataType);
        row.add(sqlDatetimeSub);
        row.add(numPrecRadix);
        
        rows.add(row);
    }
    
    /**
     * Creates the columns for the type information ResultSet.
     */
    private List<MetabaseColumnInfo> createTypeInfoColumns() {
        List<MetabaseColumnInfo> columns = new ArrayList<>();
        
        columns.add(new MetabaseColumnInfo("TYPE_NAME", "TYPE_NAME", "type/Text", null));
        columns.add(new MetabaseColumnInfo("DATA_TYPE", "DATA_TYPE", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("PRECISION", "PRECISION", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("LITERAL_PREFIX", "LITERAL_PREFIX", "type/Text", null));
        columns.add(new MetabaseColumnInfo("LITERAL_SUFFIX", "LITERAL_SUFFIX", "type/Text", null));
        columns.add(new MetabaseColumnInfo("CREATE_PARAMS", "CREATE_PARAMS", "type/Text", null));
        columns.add(new MetabaseColumnInfo("NULLABLE", "NULLABLE", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("CASE_SENSITIVE", "CASE_SENSITIVE", "type/Boolean", null));
        columns.add(new MetabaseColumnInfo("SEARCHABLE", "SEARCHABLE", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("UNSIGNED_ATTRIBUTE", "UNSIGNED_ATTRIBUTE", "type/Boolean", null));
        columns.add(new MetabaseColumnInfo("FIXED_PREC_SCALE", "FIXED_PREC_SCALE", "type/Boolean", null));
        columns.add(new MetabaseColumnInfo("AUTO_INCREMENT", "AUTO_INCREMENT", "type/Boolean", null));
        columns.add(new MetabaseColumnInfo("LOCAL_TYPE_NAME", "LOCAL_TYPE_NAME", "type/Text", null));
        columns.add(new MetabaseColumnInfo("MINIMUM_SCALE", "MINIMUM_SCALE", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("MAXIMUM_SCALE", "MAXIMUM_SCALE", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("SQL_DATA_TYPE", "SQL_DATA_TYPE", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("SQL_DATETIME_SUB", "SQL_DATETIME_SUB", "type/Integer", null));
        columns.add(new MetabaseColumnInfo("NUM_PREC_RADIX", "NUM_PREC_RADIX", "type/Integer", null));
        
        return columns;
    }
    
    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return createEmptyResultSet();
    }
    
    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        try {
            List<List<Object>> rows = new ArrayList<>();
            
            List<MetabaseColumnInfo> columns = new ArrayList<>();
            columns.add(new MetabaseColumnInfo("TYPE_CAT", "TYPE_CAT", "type/Text", null));
            columns.add(new MetabaseColumnInfo("TYPE_SCHEM", "TYPE_SCHEM", "type/Text", null));
            columns.add(new MetabaseColumnInfo("TYPE_NAME", "TYPE_NAME", "type/Text", null));
            columns.add(new MetabaseColumnInfo("CLASS_NAME", "CLASS_NAME", "type/Text", null));
            columns.add(new MetabaseColumnInfo("DATA_TYPE", "DATA_TYPE", "type/Integer", null));
            columns.add(new MetabaseColumnInfo("REMARKS", "REMARKS", "type/Text", null));
            columns.add(new MetabaseColumnInfo("BASE_TYPE", "BASE_TYPE", "type/Integer", null));
            
            MetabaseQueryResult result = 
                new MetabaseQueryResult(columns, rows);
            
            return new MetabaseResultSet(null, result);
            
        } catch (Exception e) {
            return createEmptyResultSet();
        }
    }
    
    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return createEmptyResultSet();
    }
    
    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return createEmptyResultSet();
    }
    
    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return createEmptyResultSet();
    }
    
    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return createEmptyResultSet();
    }
    
    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return createEmptyResultSet();
    }
    
    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return createEmptyResultSet();
    }
    
    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return createEmptyResultSet();
    }
    
    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }
    
    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }
    
    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 1;
    }
    
    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }
    
    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 4;
    }
    
    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 2;
    }
    
    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }
    
    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }
    
    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }
    
    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }
    
    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }
    
    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }
    
    /**
     * Creates an empty ResultSet for unimplemented metadata methods.
     * 
     * @return an empty ResultSet
     */
    private ResultSet createEmptyResultSet() {
        List<MetabaseColumnInfo> emptyColumns = new ArrayList<>();
        List<List<Object>> emptyRows = new ArrayList<>();
        MetabaseQueryResult emptyResult = 
            new MetabaseQueryResult(emptyColumns, emptyRows);
        
        return new MetabaseResultSet(null, emptyResult);
    }
} 