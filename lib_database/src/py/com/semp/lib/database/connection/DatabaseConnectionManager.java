package py.com.semp.lib.database.connection;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import py.com.semp.lib.database.configuration.DatabaseConfiguration;
import py.com.semp.lib.utilidades.exceptions.DataAccessException;

/**
 * Manages a pool of {@link DatabaseConnection} instances keyed by an identifier (e.g., thread or custom object).
 * Provides utilities to acquire, reuse, and close database connections safely.
 *
 * <p>By default, connections are stored per thread. If needed, you can supply a custom key to manage them per user or session.</p>
 *
 * @author Sergio
 */
public final class DatabaseConnectionManager
{
	
	private static final Map<Object, DatabaseConnection> connectionMap = new ConcurrentHashMap<>();
	
	private DatabaseConnectionManager()
	{
		throw new AssertionError("Do not instantiate DatabaseConnectionManager.");
	}
	
	/**
	 * Returns the default key used to associate a connection with the current thread.
	 */
	public static Object getDefaultKey()
	{
		return Thread.currentThread();
	}
	
	/**
	 * Gets or creates a connection associated with the current thread.
	 *
	 * @param configuration The database configuration to use if a new connection is needed.
	 * @return A connected {@link DatabaseConnection} instance.
	 * @throws DataAccessException If connection fails.
	 */
	public static DatabaseConnection getConnection(DatabaseConfiguration configuration) throws DataAccessException
	{
		return getConnection(getDefaultKey(), configuration);
	}
	
	/**
	 * Gets or creates a connection associated with the given key.
	 * If a connection exists and the configuration differs, it is replaced.
	 *
	 * @param key The key used to retrieve the connection.
	 * @param configuration The database configuration to use if a new connection is needed.
	 * @return A connected {@link DatabaseConnection} instance.
	 * @throws DataAccessException If connection fails.
	 */
	public static synchronized DatabaseConnection getConnection(Object key, DatabaseConfiguration configuration) throws DataAccessException
	{
		if(key == null)
		{
			throw new IllegalArgumentException("Connection key cannot be null.");
		}
		
		DatabaseConnection current = connectionMap.get(key);
		
		if(current != null && configuration.equals(current.getConfiguration()))
		{
			if(!current.isConnected())
			{
				current.connect();
			}
			return current;
		}
		
		// Replace if configuration changed or new
		if(current != null)
		{
			current.silentClose();
		}
		
		DatabaseConnection connection = new DatabaseConnection(key, configuration);
		connection.connect();
		
		connectionMap.put(key, connection);
		return connection;
	}
	
	/**
	 * Closes and removes the connection associated with the current thread.
	 */
	public static void closeConnection()
	{
		closeConnection(getDefaultKey());
	}
	
	/**
	 * Closes and removes the connection associated with the given key.
	 */
	public static void closeConnection(Object key)
	{
		if(key == null) return;
		
		DatabaseConnection connection = connectionMap.remove(key);
		if(connection != null)
		{
			connection.silentClose();
		}
	}
	
	/**
	 * Closes and removes all tracked connections.
	 */
	public static void closeAllConnections()
	{
		for(Map.Entry<Object, DatabaseConnection> entry : connectionMap.entrySet())
		{
			DatabaseConnection conn = entry.getValue();
			if(conn != null)
			{
				conn.silentClose();
			}
		}
		connectionMap.clear();
	}
	
	/**
	 * Gets the number of currently tracked connections.
	 */
	public static int getConnectionCount()
	{
		return connectionMap.size();
	}
	
	/**
	 * Returns a human-readable list of current connections.
	 */
	public static String connectionsToString()
	{
		StringBuilder sb = new StringBuilder();
		for(Map.Entry<Object, DatabaseConnection> entry : connectionMap.entrySet())
		{
			sb.append(entry.getKey()).append(" -> ").append(entry.getValue()).append("\n");
		}
		sb.append("Total: ").append(getConnectionCount());
		return sb.toString();
	}
}
