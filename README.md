# Metabase JDBC Driver

A custom JDBC driver that enables direct connection to Metabase via API, allowing tools like DBeaver, IntelliJ IDEA, and other JDBC clients to connect to databases through Metabase infrastructure with full autocomplete support.

## Overview

This driver bridges the gap between traditional SQL tools and Metabase by providing a standard JDBC interface to access databases connected to your Metabase instance. Instead of connecting directly to your databases, you can now leverage Metabase's existing connections and security layer while maintaining the familiar experience of your favorite SQL client.

## Features

- **Direct Metabase API Integration** - Connect to any database through Metabase
- **Full JDBC Compliance** - Works with any JDBC-compatible tool
- **Smart Autocomplete** - Table and column suggestions in SQL editors  
- **Intelligent Caching** - Optimized metadata loading with lazy caching
- **DBeaver Ready** - Pre-configured for seamless DBeaver integration
- **Secure Authentication** - Username/password authentication with session management
- **Custom Commands** - Built-in Metabase-specific SQL commands
- **High Performance** - Optimized API calls and connection pooling
- **Cross-Platform** - Works on Windows, macOS, and Linux

## Requirements

- Java 11 or higher
- Maven 3.6+ (for building from source)
- Access to a Metabase instance
- Valid Metabase user credentials

## Installation

### Building from Source

1. Clone the repository:
   ```bash
   git clone https://github.com/davydmaker/metabase-jdbc-driver.git
   cd metabase-jdbc-driver
   ```

2. Build the project:
   ```bash
   mvn clean package
   ```

3. The compiled JAR will be available at:
   ```
   target/metabase-jdbc-driver-1.2.1-with-dependencies.jar
   ```

### Using with DBeaver

1. Open DBeaver and go to **Database** → **Driver Manager**
2. Click **New** to create a new driver
3. Configure the driver with these settings:
   - **Driver Name**: `Metabase JDBC Driver`
   - **Driver Type**: `Generic`
   - **Class Name**: `com.davydmaker.metabase.jdbc.MetabaseDriver`
   - **URL Template**: `jdbc:metabase://{host}:{port}/{database}`
   - **Default Port**: `443` (for HTTPS) or `80` (for HTTP)

4. In the **Libraries** tab, click **Add File** and select the generated JAR file
5. Click **OK** to save the driver

### Creating a Connection

1. Click **New Database Connection** in DBeaver
2. Select **Metabase JDBC Driver** from the list
3. Configure the connection:
   - **Server Host**: Your Metabase server hostname
   - **Port**: 443 for HTTPS or your custom port
   - **Database**: The database name as configured in Metabase
   - **Username**: Your Metabase username
   - **Password**: Your Metabase password

4. Test the connection and click **Finish**

## Connection URL Format

The JDBC URL follows this pattern:

```
jdbc:metabase://hostname:port/database-name?parameter=value
```

### Examples

```bash
# HTTPS connection (default port 443)
jdbc:metabase://my-metabase.company.com/production-db?user=john@company.com&password=mypassword

# HTTP connection with custom port
jdbc:metabase://localhost:3000/development-db?user=admin&password=admin123

# With additional parameters
jdbc:metabase://metabase.example.com/analytics?user=analyst&password=secret&connectTimeout=5000
```

### Supported Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `user` | Metabase username | Required |
| `password` | Metabase password | Required |
| `connectTimeout` | Connection timeout in milliseconds | 5000 |
| `requestTimeout` | Request timeout in milliseconds | 15000 |
| `queryTimeout` | Query timeout in milliseconds | 60000 |
| `metadataTimeout` | Metadata timeout in milliseconds | 10000 |
| `authTimeout` | Authentication timeout in milliseconds | 8000 |
| `maxRetries` | Maximum retry attempts | 2 |

## Custom Commands

The driver provides special Metabase-specific commands that can be executed as regular SQL statements. These commands give you access to Metabase metadata and administration functions.

### Available Commands

#### Help and Information

```sql
-- Show all available commands
metabase help

-- Display driver version and connection info
metabase info

-- Show connection status and authentication state
metabase status
```

#### Database Management

```sql
-- List all databases in Metabase
metabase databases

-- List all schemas in the current database
metabase schemas

-- List tables in a specific schema
metabase tables schema_name
```

#### Table Inspection

```sql
-- Get detailed table structure (similar to PostgreSQL's \d command)
metabase describe schema_name.table_name
```

### Command Examples

**List all available databases:**
```sql
metabase databases
```
Output:
| id | name | engine |
|----|------|--------|
| 1 | production | postgres |
| 2 | analytics | mysql |

**Check connection status:**
```sql
metabase status
```
Output:
| property | value |
|----------|-------|
| driver_version | 1.2.1 |
| connected_database | production |
| authenticated | true |
| cache_age_seconds | 45 |

**Describe a table:**
```sql
metabase describe public.users
```
Output:
| order | name | type | not_null | primary_key | unique_key | default |
|-------|------|------|----------|-------------|------------|---------|
| 1 | id | integer | true | true | false | nextval('users_id_seq') |
| 2 | email | varchar(255) | true | false | true | null |
| 3 | created_at | timestamp | true | false | false | now() |

## Configuration

### Performance Tuning

The driver includes several configurable timeouts to optimize performance for different network conditions:

```bash
# For slow networks, increase timeouts
jdbc:metabase://server/db?connectTimeout=10000&requestTimeout=30000&queryTimeout=120000

# For fast local networks, decrease timeouts
jdbc:metabase://localhost:3000/db?connectTimeout=2000&requestTimeout=5000&queryTimeout=30000
```

### Caching Behavior

The driver implements intelligent metadata caching to reduce API calls:

- **Schema cache**: 10 minutes TTL
- **Table metadata**: Loaded on-demand and cached
- **Column information**: Single API call loads both names and details
- **Automatic refresh**: Cache refreshes when expired

## Troubleshooting

### Common Issues

**Connection failed: Authentication error**
- Verify your Metabase username and password
- Ensure the user has access to the specified database
- Check if the Metabase instance is accessible

**Connection timeout**
- Increase the `connectTimeout` parameter
- Verify network connectivity to the Metabase server
- Check if the port is correct (443 for HTTPS, 80 default for HTTP)

**Tables not showing in autocomplete**
- Wait a few seconds for metadata to load
- Try refreshing the connection in your SQL client
- Check if the user has permissions to access the database

**Query execution timeout**
- Increase the `queryTimeout` parameter for long-running queries
- Consider breaking complex queries into smaller parts
- Check Metabase server performance

## Architecture

The driver consists of several key components:

- **MetabaseDriver**: Main JDBC driver implementation
- **MetabaseConnection**: Connection management and session handling
- **MetabaseStatement**: SQL statement execution and custom command processing
- **MetabaseDatabaseMetaData**: Database metadata provider for autocomplete
- **MetabaseApiClient**: HTTP client for Metabase API communication
- **MetabaseSchemaCache**: Intelligent caching layer for metadata

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

### Development Setup

1. Clone the repository
2. Import into your IDE as a Maven project
3. Run tests with `mvn test`
4. Build with `mvn clean package`

### Code Style

- Follow standard Java conventions
- Add JavaDoc comments for public methods
- Include unit tests for new features
- Ensure all tests pass before submitting

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built for the Metabase community
- Inspired by the need for better SQL tool integration
- Uses the official Metabase API

---

**Note**: This driver is not officially affiliated with Metabase, Inc. It's a community-developed tool that uses the public Metabase API. 