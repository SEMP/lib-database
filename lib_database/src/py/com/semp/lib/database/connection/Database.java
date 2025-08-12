package py.com.semp.lib.database.connection;

import py.com.semp.lib.database.configuration.DatabaseConfiguration;
import py.com.semp.lib.utilidades.exceptions.DataAccessException;

public class Database implements AutoCloseable
{
	private Object key;
	private DatabaseConnection connection;
	private DatabaseConfiguration configuration;
	
	public Database()
	{
		super();
		
		this.getKey();
	}
	
	public Database(DatabaseConfiguration configuration)
	{
		this();
		
		this.setConfiguration(configuration);
		
		this.configuration = configuration;
	}
	
	public DatabaseConnection getConnection() throws DataAccessException
	{
		try
		{
			this.connect();
		}
		catch(Exception e)
		{
			throw new DataAccessException(e);
		}
		
		return this.connection;
	}
	
	public void setConfiguration(DatabaseConfiguration configuration)
	{
		this.configuration = configuration;
	}
	
	public DatabaseConfiguration getConfiguration()
	{
		if(this.configuration == null)
		{
			DatabaseConfiguration configuration = new DatabaseConfiguration();
			
			configuration.loadFromEnvironment();
			
			this.configuration = configuration;
		}
		
		return this.configuration;
	}
	
	private void connect() throws DataAccessException
	{
		if(this.connection == null || !this.connection.isConnected())
		{
			DatabaseConfiguration configuration = this.getConfiguration();
			
			this.connection = DatabaseConnectionManager.getConnection(key, configuration);
		}
	}
	
	public Object getKey()
	{
		if(this.key == null)
		{
			this.key = DatabaseConnectionManager.getDefaultKey();
		}
		
		return this.key;
	}
	
	@Override
	public void close() throws DataAccessException
	{
		this.closeConnection();
	}
	
	public void closeConnection() throws DataAccessException
	{
		try
		{
			if(this.key == null)
			{
				DatabaseConnectionManager.closeConnection();
			}
			else
			{
				DatabaseConnectionManager.closeConnection(this.key);
			}
		}
		finally
		{
			this.connection = null;
		}
	}
}
