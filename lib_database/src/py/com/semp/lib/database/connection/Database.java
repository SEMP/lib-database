package py.com.semp.lib.database.connection;

import py.com.semp.lib.database.configuration.DatabaseConfiguration;
import py.com.semp.lib.utilidades.exceptions.DataAccessException;

public class Database
{
	private Object key;
	private DatabaseConnection connection;
	private DatabaseConfiguration configuration;
	
	public Database()
	{
		super();
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
	
	private void connect() throws DataAccessException
	{
		if(this.connection == null || !this.connection.isConnected())
		{
			Object key = this.getKey();
			
			DatabaseConfiguration configuration = this.getConfiguration();
			
			this.connection = DatabaseConnectionManager.getConnection(key, configuration);
		}
	}
	
	private DatabaseConfiguration getConfiguration()
	{
		if(this.configuration == null)
		{
			DatabaseConfiguration configuration = new DatabaseConfiguration();
			
			configuration.loadAllFromEnvironment();
			
			this.configuration = configuration;
		}
		
		return this.configuration;
	}

	public Object getKey()
	{
		if(this.key == null)
		{
			this.key = DatabaseConnectionManager.getDefaultKey();
		}
		
		return this.key;
	}
	
	//FIXME remove test
	public static void main(String[] args) throws DataAccessException
	{
		System.out.println("Hola");
		Database db = new Database();
		
		db.getConnection();
	}
}
