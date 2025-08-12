package py.com.semp.lib.database.connection;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import py.com.semp.lib.database.configuration.DatabaseConfiguration;
import py.com.semp.lib.database.configuration.Values;
import py.com.semp.lib.database.internal.MessageUtil;
import py.com.semp.lib.database.internal.Messages;
import py.com.semp.lib.database.utilities.DBUtils;
import py.com.semp.lib.utilidades.exceptions.DataAccessException;
import py.com.semp.lib.utilidades.log.Logger;
import py.com.semp.lib.utilidades.log.LoggerManager;

/**
 * A lightweight wrapper for managing JDBC connections and operations.
 *
 * <p>
 * This class provides a simplified and consistent API for interacting with relational databases via JDBC.
 * It encapsulates connection management, configuration enforcement, transaction handling, and execution
 * of SQL statements, including support for both positional and named parameters.
 * </p>
 *
 * <h2>Features</h2>
 * <ul>
 *   <li>Configuration-based connection setup using {@link DatabaseConfiguration}</li>
 *   <li>Automatic driver loading and connection timeout handling</li>
 *   <li>Support for standard and prepared statements with type-safe parameter binding</li>
 *   <li>Named query parsing and execution with support for escaped colon syntax (e.g., {@code ::name})</li>
 *   <li>Commit, rollback, and auto-commit control</li>
 *   <li>Safe resource management and silent close functionality</li>
 *   <li>Structured logging using {@link LoggerManager}</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * DatabaseConfiguration config = new DatabaseConfiguration()
 *     .set(DATABASE_ENGINE, DatabaseEngine.POSTGRESQL)
 *     .set(DATABASE_URL, "jdbc:postgresql://localhost:5432/mydb")
 *     .set(DATABASE_USER_NAME, "user")
 *     .set(DATABASE_PASSWORD, "secret");
 *
 * DatabaseConnection db = new DatabaseConnection("myKey", config);
 * db.connect();
 *
 * String namedQuery = "UPDATE users SET name = :name WHERE id = :id";
 * Map<String, Object> params = Map.of("name", "Alice", "id", 123);
 * db.executeNamedUpdate(namedQuery, params);
 *
 * db.commit();
 * db.silentClose();
 * }</pre>
 *
 * @author Sergio Morel
 * @see DatabaseConfiguration
 * @see DBUtils
 * @see DataAccessException
 */
public final class DatabaseConnection implements AutoCloseable
{
	private Object key;
	private Connection connection;
	private DatabaseConfiguration configuration;
	private Logger logger;
	
	/**
	 * Creates a new {@code DatabaseConnection} with the given key.
	 * 
	 * <p>
	 * The connection must be configured and explicitly connected before use.
	 * A default logger is initialized using the standard database logging context.
	 * </p>
	 *
	 * @param key A unique identifier for this connection instance (used for tracking or caching purposes).
	 */
	public DatabaseConnection(Object key)
	{
		super();
		
		this.key = key;
		this.connection = null;
		this.logger = LoggerManager.getLogger(Values.Constants.DATABASE_CONTEXT);
	}
	
	/**
	 * Creates a new {@code DatabaseConnection} with the given key and configuration.
	 * 
	 * <p>
	 * The configuration is validated immediately, but no connection is established yet.
	 * A default logger is initialized using the standard database logging context.
	 * </p>
	 *
	 * @param key A unique identifier for this connection instance.
	 * @param configuration The database configuration containing all required connection parameters.
	 * @throws DataAccessException If the configuration is null or missing required parameters.
	 */
	public DatabaseConnection(Object key, DatabaseConfiguration configuration) throws DataAccessException
	{
		super();
		
		this.key = key;
		this.logger = LoggerManager.getLogger(Values.Constants.DATABASE_CONTEXT);
		this.connection = null;
		this.setConfiguration(configuration);
	}
	
	/**
	 * Creates a new {@code DatabaseConnection} with the given key, configuration, and custom logger.
	 * 
	 * <p>
	 * This constructor allows injection of a custom logger, useful for advanced logging strategies or integration
	 * with other logging frameworks.
	 * </p>
	 *
	 * @param key A unique identifier for this connection instance.
	 * @param configuration The database configuration containing all required connection parameters.
	 * @param logger A custom logger to use for all internal messages and diagnostics.
	 * @throws DataAccessException If the configuration is null or missing required parameters.
	 */
	public DatabaseConnection(Object key, DatabaseConfiguration configuration, Logger logger) throws DataAccessException
	{
		super();
		
		this.key = key;
		this.logger = logger;
		this.connection = null;
		this.setConfiguration(configuration);
	}
	
	/**
	 * Returns the unique key associated with this {@code DatabaseConnection} instance.
	 *
	 * @return The object key used to identify this connection.
	 */
	public Object getKey()
	{
		return this.key;
	}
	
	/**
	 * Validates and applies the provided {@code DatabaseConfiguration}.
	 *
	 * @param configuration The database configuration to set.
	 * @throws DataAccessException If the configuration is null or invalid (e.g., missing required parameters).
	 */
	protected void setConfiguration(DatabaseConfiguration configuration) throws DataAccessException
	{
		if(configuration == null || !configuration.checkRequiredParameters())
		{
			String errorMessage = MessageUtil.getMessage(Messages.INVALID_CONFIGURATION_ERROR, configuration);
			
			throw new DataAccessException(errorMessage);
		}
		
		this.configuration = configuration;
	}
	
	/**
	 * Sets a new {@code DatabaseConfiguration} and immediately attempts to establish the database connection.
	 *
	 * @param configuration The configuration to apply.
	 * @throws DataAccessException If the configuration is invalid or if the connection fails to be established.
	 */
	public void connect(DatabaseConfiguration configuration) throws DataAccessException
	{
		this.setConfiguration(configuration);
		
		this.connect();
	}
	
	/**
	 * Establishes a new connection using the currently configured {@code DatabaseConfiguration}.
	 * 
	 * <p>
	 * If a connection is already open, this method will raise an exception to prevent accidental reconnection.
	 * The driver is loaded using {@code Class.forName}, and the connection is established via {@code DriverManager}.
	 * Auto-commit is disabled by default.
	 * </p>
	 *
	 * @throws DataAccessException If configuration is missing, the driver class is not found,
	 *                             a connection is already open, or the connection attempt fails.
	 */
	public void connect() throws DataAccessException
	{
		if(this.isConnected())
		{
			String errorMessage = MessageUtil.getMessage(Messages.ALREADY_CONNECTED_ERROR);
			
			throw new DataAccessException(errorMessage);
		}
		
		DatabaseConfiguration configuration = this.getConfiguration();
		
		DatabaseEngine dbEngine = configuration.getValue(Values.VariableNames.DATABASE_ENGINE);
		
		String driverClass = dbEngine.getDriverClass();
		String url = configuration.getValue(Values.VariableNames.DATABASE_URL);
		Integer timeout = configuration.getValue(Values.VariableNames.CONNECTION_TIMEOUT_SECONDS);
		String user = configuration.getValue(Values.VariableNames.DATABASE_USER_NAME);
		String password = configuration.getValue(Values.VariableNames.DATABASE_PASSWORD);
		
		try
		{
			Class.forName(driverClass);
		}
		catch(ClassNotFoundException e)
		{
			String errorMessage = MessageUtil.getMessage(Messages.DATABASE_DRIVER_NOT_FOUND_ERROR, driverClass);
			
			throw new DataAccessException(errorMessage, e);
		}
		
		String debugMessage = MessageUtil.getMessage(Messages.CONNECTING_TO_DATABASE, dbEngine.name(), driverClass, timeout, url, user);
		
		this.logger.debug(debugMessage);
		
		DriverManager.setLoginTimeout(timeout);
		
		try
		{
			this.connection = DriverManager.getConnection(url, user, password);
		}
		catch(SQLException e)
		{
			throw new DataAccessException(debugMessage, e);
		}
		
		this.setAutoCommit(false);
		
		try
		{
			if(this.logger.isDebugging())
			{
				DatabaseMetaData meta = this.getMetaData(); 
				
				String message = MessageUtil.getMessage(Messages.DATABASE_CONNECTED, meta.getDatabaseProductName(), meta.getDatabaseProductVersion());
				
				this.logger.debug(message);
			}
		}
		catch(SQLException e)
		{
			this.logger.warning(e);
		}
		catch(DataAccessException e)
		{
			this.logger.warning(e);
		}
	}
	
	/**
	 * Retrieves the {@link DatabaseMetaData} for the current database connection.
	 *
	 * @return the {@code DatabaseMetaData} object that contains metadata about the database.
	 * @throws DataAccessException if the connection is not established or metadata cannot be retrieved.
	 */
	public DatabaseMetaData getMetaData() throws DataAccessException
	{
		this.assertConnected();
		
		try
		{
			return this.connection.getMetaData();
		}
		catch(SQLException e)
		{
			String errorMessage = MessageUtil.getMessage(Messages.UNABLE_T0_OBTAIN_METADATA_ERROR);
			
			throw new DataAccessException(errorMessage, e);
		}
	}
	
	/**
	 * Returns the underlying {@link Connection} object.
	 * <p>
	 * This allows advanced users to interact with JDBC features not exposed
	 * directly by this wrapper.
	 *
	 * @return the active JDBC {@link Connection}
	 * @throws DataAccessException if no active connection exists
	 */
	public Connection getConnection() throws DataAccessException
	{
		this.assertConnected();
		
		return this.connection;
	}
	
	/**
	 * Retrieves the current {@link DatabaseConfiguration} associated with this connection.
	 * <p>
	 * This method ensures that the configuration has been properly set before returning it.
	 * If the configuration is not set, a {@link DataAccessException} will be thrown.
	 * </p>
	 *
	 * @return the {@code DatabaseConfiguration} used by this connection.
	 * @throws DataAccessException if the configuration has not been initialized.
	 */
	protected DatabaseConfiguration getConfiguration() throws DataAccessException
	{
		this.assertConfigured();
		
		return this.configuration;
	}
	
	/**
	 * Closes the active database connection if it exists.
	 * <p>
	 * Logs a debug message on successful closure and sets the internal connection to {@code null}.
	 *
	 * @throws DataAccessException if an error occurs while closing the connection.
	 */
	public void closeConnection() throws DataAccessException
	{
		if(this.connection != null)
		{
			try
			{
				this.connection.close();
				
				if(this.logger.isDebugging())
				{
					String debugMessage = MessageUtil.getMessage(Messages.DATABASE_CLOSED);
					
					this.logger.debug(debugMessage);
				}
			}
			catch(SQLException e)
			{
				String errorMessage = MessageUtil.getMessage(Messages.CLOSING_DATABASE_ERROR);
				
				throw new DataAccessException(errorMessage, e);
			}
			finally
			{
				this.connection = null;
			}
		}
	}
	
	/**
	 * Silently closes the active database connection, if any.
	 * <p>
	 * Logs any {@link SQLException} as a warning instead of propagating the exception.
	 * Sets the internal connection to {@code null} after the attempt.
	 */
	public void silentClose()
	{
		if(this.connection != null)
		{
			try
			{
				this.connection.close();
				
				if(this.logger.isDebugging())
				{
					String debugMessage = MessageUtil.getMessage(Messages.DATABASE_CLOSED);
					
					this.logger.debug(debugMessage);
				}
			}
			catch(SQLException e)
			{
				String errorMessage = MessageUtil.getMessage(Messages.CLOSING_DATABASE_ERROR);
				
				this.logger.warning(errorMessage, e);
			}
			finally
			{
				this.connection = null;
			}
		}
	}
	
	/**
	 * Checks whether a database connection is currently active and open.
	 *
	 * @return {@code true} if a connection exists and is not closed; {@code false} otherwise.
	 */
	public boolean isConnected()
	{
		try
		{
			return this.connection != null && !this.connection.isClosed();
		}
		catch(SQLException e)
		{
			return false;
		}
	}
	
	/**
	 * Checks if the current database connection is still valid.
	 *
	 * @param timeoutSeconds the timeout in seconds to wait for validation
	 * @return {@code true} if the connection is valid, {@code false} otherwise
	 */
	public boolean isValid(int timeoutSeconds)
	{
		if(this.connection == null)
		{
			return false;
		}
		
		try
		{
			return this.connection.isValid(timeoutSeconds);
		}
		catch(SQLException e)
		{
			String errorMessage = MessageUtil.getMessage(Messages.CONNECTION_VALIDATION_ERROR);
			
			this.logger.warning(errorMessage, e);
			
			return false;
		}
	}
	
	/**
	 * Sets the auto-commit mode of the current database connection.
	 * <p>
	 * Logs a debug message with the new auto-commit state.
	 *
	 * @param autoCommit {@code true} to enable auto-commit mode, {@code false} to disable it.
	 * @throws DataAccessException if the connection is not active or an error occurs when setting auto-commit.
	 */
	public void setAutoCommit(boolean autoCommit) throws DataAccessException
	{
		this.assertConnected();
		
		try
		{
			this.connection.setAutoCommit(autoCommit);
			
			if(this.logger.isDebugging())
			{
				String message = MessageUtil.getMessage(Messages.SET_AUTO_COMMIT, autoCommit);
				
				this.logger.debug(message);
			}
		}
		catch(SQLException e)
		{
			String errorMessage = MessageUtil.getMessage(Messages.SET_AUTO_COMMIT_ERROR, autoCommit);
			
			throw new DataAccessException(errorMessage, e);
		}
	}
	
	/**
	 * Retrieves the current auto-commit mode of the database connection.
	 *
	 * @return {@code true} if auto-commit is enabled; {@code false} otherwise.
	 * @throws DataAccessException if the connection is not active or the value cannot be retrieved.
	 */
	public boolean getAutoCommit() throws DataAccessException
	{
		this.assertConnected();
		
		try
		{
			return this.connection.getAutoCommit();
		}
		catch(SQLException e)
		{
			String errorMessage = MessageUtil.getMessage(Messages.GET_AUTO_COMMIT_ERROR);
			
			throw new DataAccessException(errorMessage, e);
		}
	}
	
	/**
	 * Commits all changes made since the previous commit or rollback.
	 * <p>
	 * This should be called only when auto-commit mode is disabled.
	 *
	 * @throws DataAccessException if the connection is not active or an error occurs during commit.
	 */
	public void commit() throws DataAccessException
	{
		this.assertConnected();
		
		try
		{
			this.connection.commit();
		}
		catch(SQLException e)
		{
			String errorMessage = MessageUtil.getMessage(Messages.COMMIT_ERROR);
			
			throw new DataAccessException(errorMessage, e);
		}
	}
	
	/**
	 * Rolls back all changes made since the previous commit or rollback.
	 * <p>
	 * This should be called only when auto-commit mode is disabled.
	 *
	 * @throws DataAccessException if the connection is not active or an error occurs during rollback.
	 */
	public void rollback() throws DataAccessException
	{
		this.assertConnected();
		
		try
		{
			this.connection.rollback();
		}
		catch(SQLException e)
		{
			String errorMessage = MessageUtil.getMessage(Messages.ROLLBACK_ERROR);
			
			throw new DataAccessException(errorMessage, e);
		}
	}
	
	/**
	 * Creates a new {@link Statement} for sending SQL commands to the database.
	 * <p>
	 * If {@code DATABASE_FETCH_SIZE} is configured, it will be set on the created statement.
	 * <p>
	 * <strong>Note:</strong> This method creates a {@link Statement} with default result set type and concurrency:
	 * {@link ResultSet#TYPE_FORWARD_ONLY} and {@link ResultSet#CONCUR_READ_ONLY}. This is optimal for large, streaming
	 * result sets when combined with a non-zero fetch size (e.g., for PostgreSQL cursor-based streaming).
	 * If scrollable or updatable results are required, consider using {@link Connection#createStatement(int, int)} directly.
	 *
	 * @return a new {@link Statement} instance
	 * @throws DataAccessException if the database is not configured or connected, or if an error occurs during creation
	 */
	public Statement createStatement() throws DataAccessException
	{
		this.assertConfigured();
		this.assertConnected();
		
		Statement statement = null;
		
		try
		{
			statement = this.connection.createStatement();
			
			Integer fetchSize = this.configuration.getValue(Values.VariableNames.DATABASE_FETCH_SIZE);
			
			if(fetchSize != null)
			{
				statement.setFetchSize(fetchSize);
			}
			
			return statement;
		}
		catch(SQLException e)
		{
			if(statement != null)
			{
				try
				{
					statement.close();
				}
				catch(SQLException closeException)
				{
					e.addSuppressed(closeException);
					
				    this.logger.warning(closeException);
				}
			}
			
			String errorMessage = MessageUtil.getMessage(Messages.CREATING_STATEMENT_ERROR);
			
			throw new DataAccessException(errorMessage, e);
		}
	}
	
	/**
	 * Creates a new {@link PreparedStatement} for the given SQL query.
	 * <p>
	 * If {@code DATABASE_FETCH_SIZE} is configured, it will be applied to the statement.
	 * <p>
	 * <strong>Note:</strong> This creates a forward-only, read-only {@link PreparedStatement} by default.
	 * This is efficient for large result sets when paired with fetch size tuning. For scrollable or updatable
	 * prepared statements, use {@link Connection#prepareStatement(String, int, int)} instead.
	 *
	 * @param sql the SQL query to prepare
	 * @return a new {@link PreparedStatement} instance
	 * @throws DataAccessException if the database is not configured or connected, or if an error occurs during preparation
	 */
	public PreparedStatement createPreparedStatement(String sql) throws DataAccessException
	{
		this.assertConfigured();
		this.assertConnected();
		
		PreparedStatement preparedStatement = null;
		
		try
		{
			preparedStatement = this.connection.prepareStatement(sql);
			
			Integer fetchSize = this.configuration.getValue(Values.VariableNames.DATABASE_FETCH_SIZE);
			
			if(fetchSize != null)
			{
				preparedStatement.setFetchSize(fetchSize);
			}
			
			return preparedStatement;
		}
		catch(SQLException e)
		{
			if(preparedStatement != null)
			{
				try
				{
					preparedStatement.close();
				}
				catch(SQLException closeException)
				{
					e.addSuppressed(closeException);
					
				    this.logger.warning(closeException);
				}
			}
			
			String errorMessage = MessageUtil.getMessage(Messages.CREATING_PREPARED_STATEMENT_ERROR, sql);
			
			throw new DataAccessException(errorMessage, e);
		}
	}
	
	/**
	 * Verifies that a database connection is currently active.
	 * <p>
	 * This method should be used in subclasses before attempting to access
	 * or manipulate the underlying {@link java.sql.Connection}.
	 * </p>
	 *
	 * @throws DataAccessException if the connection is {@code null} or has been closed.
	 * 
	 * @see #isConnected()
	 */
	protected void assertConnected() throws DataAccessException
	{
		if(!this.isConnected())
		{
			String errorMessage = MessageUtil.getMessage(Messages.DATABASE_NOT_CONNECTED_ERROR);
			
			throw new DataAccessException(errorMessage);
		}
	}
	
	/**
	 * Verifies that the {@link DatabaseConfiguration} has been set and is valid.
	 * <p>
	 * This method should be called before performing any operation that
	 * relies on configuration values (such as driver class, URL, credentials, etc.).
	 * Subclasses may use this to ensure preconditions are satisfied.
	 * </p>
	 *
	 * @throws DataAccessException if the configuration is {@code null}.
	 */
	protected void assertConfigured() throws DataAccessException
	{
		if(this.configuration == null)
		{
			String errorMessage = MessageUtil.getMessage(Messages.DATABASE_NOT_CONFIGURED_ERROR);
			
			throw new DataAccessException(errorMessage);
		}
	}
	
	/**
	 * Executes the given SQL statement, which must be an SQL Data Manipulation Language (DML) statement 
	 * such as INSERT, UPDATE, or DELETE.
	 * <p>
	 * A standard {@link java.sql.Statement} is used to execute the SQL directly, without parameters.
	 * </p>
	 *
	 * @param sql the SQL statement to execute.
	 * @return the number of rows affected by the execution.
	 * @throws DataAccessException if a database access error occurs or the statement is invalid.
	 * 
	 * @see java.sql.Statement#executeUpdate(String)
	 * @see #createStatement()
	 */
	public int executeUpdate(String sql) throws DataAccessException
	{
		try(Statement statement = this.createStatement())
		{
			return statement.executeUpdate(sql);
		}
		catch(SQLException e)
		{
			String errorMessage = MessageUtil.getMessage(Messages.QUERY_EXECUTION_ERROR, sql);
			
			throw new DataAccessException(errorMessage, e);
		}
	}
	
	/**
	 * Executes the given SQL DML statement with the provided positional parameters.
	 * <p>
	 * A {@link java.sql.PreparedStatement} is created and populated using the specified parameters. 
	 * Parameter placeholders in the SQL should be marked with {@code ?} and must match the number of arguments provided.
	 * </p>
	 *
	 * @param sql the SQL statement to execute, containing {@code ?} placeholders for parameters.
	 * @param parameters the ordered parameters to bind to the SQL statement.
	 * @return the number of rows affected by the execution.
	 * @throws DataAccessException if a database access error occurs or the statement is invalid.
	 *
	 * @see java.sql.PreparedStatement#executeUpdate()
	 * @see #createPreparedStatement(String)
	 * @see #setParameterStatement(PreparedStatement, int, Object)
	 */
	public int executeUpdate(String sql, Object... parameters) throws DataAccessException
	{
		try(PreparedStatement preparedStatement = this.createPreparedStatement(sql))
		{
			for (int i = 0; i < parameters.length; i++)
			{
				setParameterStatement(preparedStatement, i, parameters[i]);
			}
			
			return preparedStatement.executeUpdate();
		}
		catch(SQLException e)
		{
			String errorMessage = MessageUtil.getMessage(Messages.QUERY_EXECUTION_ERROR, sql);
			
			throw new DataAccessException(errorMessage, e);
		}
	}
	
	/**
	 * Executes an update statement (e.g., INSERT, UPDATE, DELETE) using a named parameter SQL string.
	 * Named parameters in the form of {@code :name} will be replaced with {@code ?} placeholders,
	 * and their corresponding values will be retrieved from the provided map.
	 *
	 * @param namedQuery The SQL string with named parameters (e.g., {@code UPDATE users SET name = :name WHERE id = :id}).
	 * @param valuesMap A map containing parameter names and their corresponding values.
	 * @return The number of rows affected by the update.
	 * @throws DataAccessException If the SQL is invalid, a parameter is missing, or a database access error occurs.
	 */
	public int executeNamedUpdate(String namedQuery, Map<String, Object> valuesMap) throws DataAccessException
	{
		List<Object> values = new LinkedList<>();
		
		String jdbcSQL = DBUtils.parseNamedQueryToJDBC(namedQuery, valuesMap, values);
		
		return this.executeUpdate(jdbcSQL, values.toArray());
	}
	
	/**
	 * Executes the given SQL query, which should return a single {@link java.sql.ResultSet}.
	 * <p>
	 * This method uses a {@link java.sql.Statement} to execute the SQL directly, without any bound parameters.
	 * The caller is responsible for processing and closing the returned {@code ResultSet}.
	 * </p>
	 *
	 * @param sql the SQL query to be executed.
	 * @return the resulting {@code ResultSet} from the query execution.
	 * @throws DataAccessException if a database access error occurs or the statement is invalid.
	 *
	 * @see java.sql.Statement#executeQuery(String)
	 * @see #createStatement()
	 */
	public ResultSet executeQuery(String sql) throws DataAccessException
	{
		try(Statement statement = this.createStatement())
		{
			return statement.executeQuery(sql);
		}
		catch(SQLException e)
		{
			String errorMessage = MessageUtil.getMessage(Messages.QUERY_EXECUTION_ERROR, sql);
			
			throw new DataAccessException(errorMessage, e);
		}
	}
	
	/**
	 * Executes the given SQL query with positional parameters, returning a {@link java.sql.ResultSet}.
	 * <p>
	 * This method creates a {@link java.sql.PreparedStatement}, binds the provided parameters to the query,
	 * and executes it. Placeholders in the SQL string must be represented by {@code ?} characters, and the
	 * number of parameters must match the number of placeholders.
	 * </p>
	 * <p>
	 * The caller is responsible for processing and closing the returned {@code ResultSet}.
	 * </p>
	 *
	 * @param sql the SQL query containing {@code ?} placeholders for parameters.
	 * @param parameters the values to bind to the query in order.
	 * @return the resulting {@code ResultSet} from the query execution.
	 * @throws DataAccessException if a database access error occurs or the statement is invalid.
	 *
	 * @see java.sql.PreparedStatement#executeQuery()
	 * @see #createPreparedStatement(String)
	 * @see #setParameterStatement(PreparedStatement, int, Object)
	 */
	public ResultSet executeQuery(String sql, Object... parameters) throws DataAccessException
	{
		try(PreparedStatement preparedStatement = this.createPreparedStatement(sql))
		{
			for (int i = 0; i < parameters.length; i++)
			{
				setParameterStatement(preparedStatement, i, parameters[i]);
			}
			
			return preparedStatement.executeQuery();
		}
		catch(SQLException e)
		{
			String errorMessage = MessageUtil.getMessage(Messages.QUERY_EXECUTION_ERROR, sql);
			
			throw new DataAccessException(errorMessage, e);
		}
	}
	
	/**
	 * Executes a query statement (e.g., SELECT) using a named parameter SQL string.
	 * Named parameters in the form of {@code :name} will be replaced with {@code ?} placeholders,
	 * and their corresponding values will be retrieved from the provided map.
	 *
	 * @param namedQuery The SQL string with named parameters (e.g., {@code SELECT * FROM users WHERE status = :status}).
	 * @param valuesMap A map containing parameter names and their corresponding values.
	 * @return A {@link ResultSet} containing the data produced by the query.
	 * @throws DataAccessException If the SQL is invalid, a parameter is missing, or a database access error occurs.
	 */
	public ResultSet executeNamedQuery(String namedQuery, Map<String, Object> valuesMap) throws DataAccessException
	{
		List<Object> values = new LinkedList<>();
		
		String jdbcSQL = DBUtils.parseNamedQueryToJDBC(namedQuery, valuesMap, values);
		
		return this.executeQuery(jdbcSQL, values.toArray());
	}
	
	/**
	 * Sets a parameter value in the given {@link PreparedStatement}, inferring the correct setter method
	 * based on the runtime type of the parameter.
	 * <p>
	 * This method wraps various {@code PreparedStatement.setXXX()} calls, including support for standard
	 * types such as {@link String}, numeric wrappers, {@link java.util.Date}, {@link java.sql.Date}, 
	 * {@link java.sql.Timestamp}, {@code byte[]} and {@link Boolean}. If the parameter type is not explicitly
	 * handled, it defaults to {@code setObject}.
	 * </p>
	 * <p>
	 * The parameter index is zero-based for usability, but internally adjusted to JDBC's one-based index.
	 * </p>
	 *
	 * @param ps the {@code PreparedStatement} in which to set the parameter.
	 * @param index the zero-based index of the parameter to set.
	 * @param parameter the value to set; may be {@code null}.
	 * @throws DataAccessException if a {@link SQLException} occurs while setting the parameter.
	 *
	 * @see PreparedStatement#setString(int, String)
	 * @see PreparedStatement#setInt(int, int)
	 * @see PreparedStatement#setObject(int, Object)
	 * @see java.sql.Types
	 */
	public static void setParameterStatement(PreparedStatement ps, int index, Object parameter) throws DataAccessException
	{
		int i = index + 1;
		
		try
		{
			if(parameter instanceof String)
			{
				ps.setString(i, (String)parameter);
			}
			else if(parameter instanceof Short)
			{
				ps.setShort(i, (Short)parameter);
			}
			else if(parameter instanceof Integer)
			{
				ps.setInt(i, (Integer)parameter);
			}
			else if(parameter instanceof Long)
			{
				ps.setLong(i, (Long)parameter);
			}
			else if(parameter instanceof Float)
			{
				ps.setFloat(i, (Float)parameter);
			}
			else if(parameter instanceof Double)
			{
				ps.setDouble(i, (Double)parameter);
			}
			else if(parameter instanceof BigDecimal)
			{
				ps.setBigDecimal(i, (BigDecimal)parameter);
			}
			else if(parameter instanceof java.sql.Date)
			{
				ps.setDate(i, (java.sql.Date)parameter);
			}
			else if(parameter instanceof java.sql.Time)
			{
				ps.setTime(i, (java.sql.Time)parameter);
			}
			else if(parameter instanceof java.sql.Timestamp)
			{
				ps.setTimestamp(i, (java.sql.Timestamp)parameter);
			}
			else if(parameter instanceof java.util.Date)
			{
				java.util.Date fecha = (java.util.Date)parameter;
				java.sql.Timestamp sqlTimeStamp = new java.sql.Timestamp(fecha.getTime());
				ps.setTimestamp(i, sqlTimeStamp);
			}
			else if(parameter instanceof byte[])
			{
				ps.setBytes(i, (byte[])parameter);
			}
			else if(parameter instanceof Boolean)
			{
				ps.setBoolean(i, (Boolean)parameter);
			}
			else
			{
				ps.setObject(i, parameter);
			}
		}
		catch (SQLException e)
		{
			String errorMessage = MessageUtil.getMessage(Messages.SETTING_STATEMENT_PARAMETER_ERROR, index);
			
			throw new DataAccessException(errorMessage, e);
		}
	}
	
	@Override
	public boolean equals(Object obj)
	{
		if(this == obj)
		{
			return true;
		}
		
		if(obj == null || !obj.getClass().equals(this.getClass()))
		{
			return false;
		}
		
		DatabaseConnection other = (DatabaseConnection)obj;
		
		if(this.configuration == other.configuration)
		{
			return true;
		}
		
		if(this.configuration == null || other.configuration == null)
		{
			return false;
		}
		
		DatabaseEngine dbEngine = this.configuration.getValue(Values.VariableNames.DATABASE_ENGINE);
		String url = this.configuration.getValue(Values.VariableNames.DATABASE_URL);
		String user = this.configuration.getValue(Values.VariableNames.DATABASE_USER_NAME);
		
		DatabaseEngine otherDbEngine = other.configuration.getValue(Values.VariableNames.DATABASE_ENGINE);
		String otherUrl = other.configuration.getValue(Values.VariableNames.DATABASE_URL);
		String otherUser = other.configuration.getValue(Values.VariableNames.DATABASE_USER_NAME);
		
		return Objects.equals(dbEngine, otherDbEngine)
			    && Objects.equals(url, otherUrl)
			    && Objects.equals(user, otherUser);
	}
	
	@Override
	public int hashCode()
	{
		if(this.configuration == null)
		{
			return 0;
		}
		
		return Objects.hash
		(
			this.configuration.getValue(Values.VariableNames.DATABASE_ENGINE),
			this.configuration.getValue(Values.VariableNames.DATABASE_URL),
			this.configuration.getValue(Values.VariableNames.DATABASE_USER_NAME)
		);
	}
	
	/**
	 * Returns a detailed multi-line string representation of this {@code DatabaseConnection},
	 * including its class name, connection status, and the full configuration object.
	 * <p>
	 * This method is intended for debugging or logging purposes where full visibility
	 * into the connection's configuration is useful.
	 *
	 * @return a detailed string describing the connection and its configuration
	 */
	public String getConnectionDetails()
	{
		StringBuilder sb = new StringBuilder();
		
		boolean connected = this.isConnected();
		
		sb.append(this.getClass().getSimpleName());
		sb.append(" [");
		sb.append(connected ? "✔" : "✘");
		sb.append("]:\n");
		sb.append(this.configuration);
		
		return sb.toString();
	}
	
	/**
	 * Returns a concise, single-line string representation of this {@code DatabaseConnection},
	 * including its class name, connection status, and the database URL (if available).
	 * <p>
	 * The connection status is shown as {@code ✔} if connected, or {@code ✘} if not.
	 * If the configuration or URL is missing, {@code (no url)} is displayed instead.
	 *
	 * @return a short string summarizing the connection state and URL
	 */
	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		
		boolean connected = this.isConnected();
		
		sb.append(this.getClass().getSimpleName());
		sb.append(" [");
		sb.append(connected ? "✔" : "✘");
		sb.append("]: ");
		
		if(this.configuration != null)
		{
			String url = this.configuration.getValue(Values.VariableNames.DATABASE_URL);
			
			sb.append(url != null ? url : "(no url)");
		}
		
		return sb.toString();
	}
	
	@Override
	public void close() throws DataAccessException
	{
		this.closeConnection();
	}
}