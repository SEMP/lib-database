package py.com.semp.lib.database.configuration;

import static py.com.semp.lib.database.configuration.Values.VariableNames.*;

import py.com.semp.lib.database.connection.DatabaseEngine;
import py.com.semp.lib.utilidades.configuration.ConfigurationValues;

public class DatabaseConfiguration extends ConfigurationValues
{
	public DatabaseConfiguration()
	{
		super();
	}
	
	@Override
	protected void setRequiredParameters()
	{
		this.addRequiredParameter(String.class, DATABASE_URL);
		this.addRequiredParameter(String.class, DATABASE_USER_NAME);
		this.addRequiredParameter(String.class, DATABASE_PASSWORD);
		this.addRequiredParameter(DatabaseEngine.class, DATABASE_ENGINE);
		this.addRequiredParameter(Integer.class, CONNECTION_TIMEOUT_SECONDS);
	}
	
	@Override
	protected void setOptionalParameters()
	{
	}
	
	@Override
	protected void setDefaultValues()
	{
		this.setParameter(DatabaseEngine.class, DATABASE_ENGINE, Values.Defaults.DATABASE_ENGINE);
		this.setParameter(Integer.class, CONNECTION_TIMEOUT_SECONDS, Values.Defaults.CONNECTION_TIMEOUT_SECONDS);
	}
}