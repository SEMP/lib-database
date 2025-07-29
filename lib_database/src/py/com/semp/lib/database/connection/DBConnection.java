package py.com.semp.lib.database.connection;

import java.lang.invoke.ClassSpecializer.Factory;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import py.com.lib.database.service.ActiveStatements;
import py.com.lib.database.sql.SQLStatement;
import py.com.lib.database.util.DBFilters;
import py.com.lib.util.log.Debug;
import py.com.lib.util.values.Variables;
import py.com.semp.lib.database.configuration.DatabaseConfiguration;
import py.com.semp.lib.database.configuration.Values;
import py.com.semp.lib.database.internal.MessageUtil;
import py.com.semp.lib.database.internal.Messages;
import py.com.semp.lib.utilidades.exceptions.DataAccessException;
import py.com.semp.lib.utilidades.log.Logger;
import py.com.semp.lib.utilidades.log.LoggerManager;

//TODO verificar null para auto commit, debug de meta datos al conectar

/**
 * Wrapper de conexi&oacute;n a base de datos.
 * 
 * @author Sergio Morel
 */
public final class DBConnection
{
	private static final Logger LOGGER = LoggerManager.getLogger(Values.Constants.DATABASE_CONTEXT);
	
	private Object key;
	private Connection connection;
	private DatabaseConfiguration configuration;
	
	public DBConnection(Object key)
	{
		super();
		
		this.key = key;
		this.connection = null;
	}
	
	public DBConnection(Object key, DatabaseConfiguration configuration)
	{
		super();
		
		this.connection = null;
		this.configuration = configuration;
		this.key = key;
	}
	
	public void connect(DatabaseConfiguration configuration)
	{
		if(!configuration.checkRequiredParameters())
		{
			String errorMessage = MessageUtil.getMessage(Messages.DATABASE_NOT_CONFIGURED_ERROR);
			
			throw new DataAccessException(errorMessage);
		}
		
		this.configuration = configuration;
		
		this.connect();
	}
	
	public void connect() throws DataAccessException
	{
		if(this.configuration == null)
		{
			String errorMessage = MessageUtil.getMessage(Messages.DATABASE_NOT_CONFIGURED_ERROR);
			
			throw new DataAccessException(errorMessage);
		}
		
		DatabaseEngine dbEngine = this.configuration.getValue(Values.VariableNames.DATABASE_ENGINE);
		
		String driverClass = dbEngine.getDriverClass();
		String url = this.configuration.getValue(Values.VariableNames.DATABASE_URL);
		Integer timeout = this.configuration.getValue(Values.VariableNames.CONNECTION_TIMEOUT_SECONDS);
		String user = this.configuration.getValue(Values.VariableNames.DATABASE_USER_NAME);
		String password = this.configuration.getValue(Values.VariableNames.DATABASE_PASSWORD);
		
		try
		{
			Class.forName(driverClass);
		}
		catch(ClassNotFoundException e)
		{
			String errorMessage = MessageUtil.getMessage(Messages.DATABASE_DRIVER_NOT_FOUND_ERROR, driverClass);
			
			throw new DataAccessException(errorMessage, e);
		}
		
		String message = MessageUtil.getMessage(Messages.CONNECTING_TO_DATABASE, dbEngine.name(), driverClass, timeout, url, user);
		
		LOGGER.debug(message);
		
		DriverManager.setLoginTimeout(timeout);
		
		try
		{
			this.connection = DriverManager.getConnection(url, user, password);
		}
		catch(SQLException e)
		{
			throw new DataAccessException(message, e);
		}
		
		this.setAutoCommit(false);
	}
	
	void closeConnection() throws SQLException
	{
		if(this.connection != null && !this.connection.isClosed())
		{
			this.connection.close();
		}
	}
	
	public int executeUpdate(String sql) throws SQLException
	{
		Statement stm = this.connection.createStatement();
		
		ActiveStatements.addStatement(this.getKey(), stm);
		
		int rowCount = stm.executeUpdate(sql);
		stm.close();
		
		ActiveStatements.removeStatement(this.getKey(), stm);
		
		return rowCount;
	}
	
	public Object getKey()
	{
		return this.key;
	}
	
	public int executeUpdate(SQLStatement sqlStatement) throws SQLException
	{
		return this.executeUpdate(sqlStatement.toPreparedString(), sqlStatement.getFieldValues());
	}
	
	public int executeUpdate(String sql, Object... parameters) throws SQLException
	{
		if(DBFilters.isNumberedQuery(sql))
		{
			return this.executeNumberedUpdate(sql, parameters);
		}
		else if(DBFilters.isNamedQuery(sql))
		{
			throw new IllegalArgumentException(NAMED_STATEMENT_ERROR);
		}
		else
		{
			PreparedStatement ps = this.connection.prepareStatement(sql);
			
			for (int i = 0; i < parameters.length; i++)
			{
				setParameterStatement(ps, i, parameters[i]);
			}
			
			ActiveStatements.addStatement(this.getKey(), ps);
			
			int rowCount = ps.executeUpdate();
			ps.close();
			
			ActiveStatements.removeStatement(this.getKey(), ps);
			
			return rowCount;
		}
	}
	
	public int executeNumberedUpdate(String sql, Object[] parameters) throws SQLException
	{
		List<Integer> indices = Factory.getLinkedList();
		
		String newSql = translateNumberedQuery(sql, indices);
		
		PreparedStatement ps = this.connection.prepareStatement(newSql);
		
		for(int i = 0; i < indices.size(); i++)
		{
			Integer parameterIndex = indices.get(i) - 1;
			
			setParameterStatement(ps, i, parameters[parameterIndex]);
		}
		
		ActiveStatements.addStatement(this.getKey(), ps);
		
		int rowCount = ps.executeUpdate();
		ps.close();
		
		ActiveStatements.removeStatement(this.getKey(), ps);
		
		return rowCount;
	}
	
	public int executeNamedUpdate(String sql, Map<String, Object> valuesMap) throws SQLException
	{
		List<String> parametersName = Factory.getLinkedList();
		
		String newSql = DBFilters.translateNamedQuery(sql, parametersName);
		
		PreparedStatement ps = this.connection.prepareStatement(newSql);
		
		for(int i = 0; i < parametersName.size(); i++)
		{
			String parameterName = parametersName.get(i);
			
			setParameterStatement(ps, i, valuesMap.get(parameterName));
		}
		
		ActiveStatements.addStatement(this.getKey(), ps);
		
		int rowCount = ps.executeUpdate();
		ps.close();
		
		ActiveStatements.removeStatement(this.getKey(), ps);
		
		return rowCount;
	}
	
	public ResultSet executeQuery(String sql) throws SQLException
	{
		Statement stm = this.connection.createStatement();
		
		ActiveStatements.addStatement(this.getKey(), stm);
		
		ResultSet resultSet = stm.executeQuery(sql);
		
		ActiveStatements.removeStatement(this.getKey(), stm);
		
		return resultSet;
	}
	
	public ResultSet executeQuery(int fetchSize, String sql) throws SQLException
	{
		Statement stm = this.connection.createStatement();
		
		stm.setFetchSize(fetchSize);
		
		ActiveStatements.addStatement(this.getKey(), stm);
		
		ResultSet resultSet = stm.executeQuery(sql);
		
		ActiveStatements.removeStatement(this.getKey(), stm);
		
		return resultSet;
	}
	
	public ResultSet executeQuery(SQLStatement sqlStatement) throws SQLException
	{
		return this.executeQuery(sqlStatement.toPreparedString(), sqlStatement.getFieldValues());
	}
	
	public ResultSet executeQuery(int fetchSize, SQLStatement sqlStatement) throws SQLException
	{
		return this.executeQuery(fetchSize, sqlStatement.toPreparedString(), sqlStatement.getFieldValues());
	}
	
	public ResultSet executeQuery(String sql, Object... parameters) throws SQLException
	{
		if(DBFilters.isNumberedQuery(sql))
		{
			return this.executeNumberedQuery(sql, parameters);
		}
		else if(DBFilters.isNamedQuery(sql))
		{
			throw new IllegalArgumentException(NAMED_STATEMENT_ERROR);
		}
		else
		{
			PreparedStatement ps = this.connection.prepareStatement(sql);
			
			for (int i = 0; i < parameters.length; i++)
			{
				setParameterStatement(ps, i, parameters[i]);
			}
			
			ActiveStatements.addStatement(this.getKey(), ps);
			
			ResultSet resultSet = ps.executeQuery();
			
			ActiveStatements.removeStatement(this.getKey(), ps);
			
			return resultSet;
		}
	}
	
	public ResultSet executeQuery(int fetchSize, String sql, Object... parameters) throws SQLException
	{
		if(DBFilters.isNumberedQuery(sql))
		{
			return this.executeNumberedQuery(sql, parameters);
		}
		else if(DBFilters.isNamedQuery(sql))
		{
			throw new IllegalArgumentException(NAMED_STATEMENT_ERROR);
		}
		else
		{
			PreparedStatement ps = this.connection.prepareStatement(sql);
			
			for (int i = 0; i < parameters.length; i++)
			{
				setParameterStatement(ps, i, parameters[i]);
			}
			
			ps.setFetchSize(fetchSize);
			
			ActiveStatements.addStatement(this.getKey(), ps);
			
			ResultSet resultSet = ps.executeQuery();
			
			ActiveStatements.removeStatement(this.getKey(), ps);
			
			return resultSet;
		}
	}
	
	public ResultSet executeNumberedQuery(String sql, Object... parameters) throws SQLException
	{
		List<Integer> indices = Factory.getLinkedList();
		
		String newSql = translateNumberedQuery(sql, indices);
		
		PreparedStatement ps = this.connection.prepareStatement(newSql);
		
		for(int i = 0; i < indices.size(); i++)
		{
			Integer parameterIndex = indices.get(i) - 1;
			
			setParameterStatement(ps, i, parameters[parameterIndex]);
		}
		
		ActiveStatements.addStatement(this.getKey(), ps);
		
		ResultSet resultSet = ps.executeQuery();
		
		ActiveStatements.removeStatement(this.getKey(), ps);
		
		return resultSet; 
	}
	
	public ResultSet executeNumberedQuery(int fetchSize, String sql, Object... parameters) throws SQLException
	{
		List<Integer> indices = Factory.getLinkedList();
		
		String newSql = translateNumberedQuery(sql, indices);
		
		PreparedStatement ps = this.connection.prepareStatement(newSql);
		
		for(int i = 0; i < indices.size(); i++)
		{
			Integer parameterIndex = indices.get(i) - 1;
			
			setParameterStatement(ps, i, parameters[parameterIndex]);
		}
		
		ps.setFetchSize(fetchSize);
		
		ActiveStatements.addStatement(this.getKey(), ps);
		
		ResultSet resultSet = ps.executeQuery();
		
		ActiveStatements.removeStatement(this.getKey(), ps);
		
		return resultSet;
	}
	
	public ResultSet executeNamedQuery(String sql, Map<String, Object> valuesMap) throws SQLException
	{
		List<String> parametersName = Factory.getLinkedList();
		
		String newSql = DBFilters.translateNamedQuery(sql, parametersName);
		
		PreparedStatement ps = this.connection.prepareStatement(newSql);
		
		for(int i = 0; i < parametersName.size(); i++)
		{
			String parameterName = parametersName.get(i);
			
			setParameterStatement(ps, i, valuesMap.get(parameterName));
		}
		
		ActiveStatements.addStatement(this.getKey(), ps);
		
		ResultSet resultSet = ps.executeQuery();
		
		ActiveStatements.removeStatement(this.getKey(), ps);
		
		return resultSet;
	}
	
	public ResultSet executeNamedQuery(int fetchSize, String sql, Map<String, Object> valuesMap) throws SQLException
	{
		List<String> parametersName = Factory.getLinkedList();
		
		String newSql = DBFilters.translateNamedQuery(sql, parametersName);
		
		PreparedStatement ps = this.connection.prepareStatement(newSql);
		
		for(int i = 0; i < parametersName.size(); i++)
		{
			String parameterName = parametersName.get(i);
			
			setParameterStatement(ps, i, valuesMap.get(parameterName));
		}
		
		ps.setFetchSize(fetchSize);
		
		ActiveStatements.addStatement(this.getKey(), ps);
		
		ResultSet resultSet = ps.executeQuery();
		
		ActiveStatements.removeStatement(this.getKey(), ps);
		
		return resultSet;
	}
	
	private String translateNumberedQuery(String sql, List<Integer> indices) throws SQLException
	{
		String translatedSQL = DBFilters.removeSQLComments(sql);
		
		List<Integer> jumpIndices = Factory.getArrayList();
		
		boolean betweenSingleQuotes = false;
		boolean betweenDoubleQuotes = false;
		
		for(int i = 0; i < translatedSQL.length(); i++)
		{
			if(translatedSQL.charAt(i) == '\\')
			{
				i++;
				continue;
			}
			
			if(translatedSQL.charAt(i) == '\'' && !betweenDoubleQuotes)
			{
				betweenSingleQuotes = !betweenSingleQuotes;
			}
			
			if(translatedSQL.charAt(i) == '\"' && !betweenSingleQuotes)
			{
				betweenDoubleQuotes = !betweenDoubleQuotes;
			}
			
			if(!betweenSingleQuotes && !betweenDoubleQuotes)
			{
				if(translatedSQL.charAt(i) == '?')
				{
					String namedRegex = "^\\?\\d+.*";
					
					if(!translatedSQL.substring(i).matches(namedRegex))
					{
						throw new SQLException("Malformed Numbered Statement:\n" + translatedSQL);
					}
					
					if(jumpIndices.isEmpty())
					{
						jumpIndices.add(0);
						jumpIndices.add(i + 1);
					}
					else
					{
						jumpIndices.add(i + 1);
					}
					
					StringBuilder indexString = new StringBuilder();
					
					for(i = i + 1; i < translatedSQL.length() && String.valueOf(translatedSQL.charAt(i)).matches("\\d"); i++)
					{
						indexString.append(translatedSQL.charAt(i));
					}
					
					Integer index = Integer.valueOf(indexString.toString());
					
					indices.add(index);
					
					jumpIndices.add(i);
				}
			}
		}
		
		StringBuilder newSql = new StringBuilder();
		
		if(jumpIndices.isEmpty())
		{
			return translatedSQL;
		}
		
		for(int i = 0; i < jumpIndices.size(); i += 2)
		{
			int indiceInicial = jumpIndices.get(i);
			int indiceFinal = translatedSQL.length();
			
			if(i + 1 < jumpIndices.size())
			{
				indiceFinal = jumpIndices.get(i + 1);
			}
			
			newSql.append(translatedSQL.substring(indiceInicial, indiceFinal));
		}
		
		Debug.append(Values.Constants.DATABASE_CONTEXT, newSql);
		
		return newSql.toString();
	}
	
	public DatabaseMetaData getMetaData() throws SQLException
	{
		return this.connection.getMetaData();
	}
	
	/**
	 * Obtiene un prepared statement con la instrucci&oacute;n sql pasada por par&aacute;metro.
	 * @param sql
	 * instrucci&oacute;n sql.
	 * @return
	 * prepared statement
	 * @throws SQLException
	 * en caso de ocurrir alg&uacute;n error con la base de datos.
	 */
	public PreparedStatement prepareStatement(String sql) throws SQLException
	{
		return this.connection.prepareStatement(sql);
	}
	
	/**
	 * Establece el valor en el prepared statement de acuerdo al tipo de dato del mismo.
	 * @param ps
	 * prepared statement.
	 * @param index
	 * posicion donde establecer el valor. (desde 0)
	 * @param parameter
	 * valor que se quiere establecer.
	 * @throws SQLException
	 * en caso de ocurrir alg&uacute;n error con la base de datos.
	 */
	public static void setParameterStatement(PreparedStatement ps, int index, Object parameter) throws SQLException
	{
		int i = index + 1;
		
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
	
	public void setAutoCommit(boolean autoCommit) throws DataAccessException
	{
		try
		{
			this.connection.setAutoCommit(autoCommit);
			
			String message = MessageUtil.getMessage(Messages.SET_AUTO_COMMIT, autoCommit);
			
			LOGGER.debug(message);
		}
		catch(SQLException e)
		{
			String errorMessage = MessageUtil.getMessage(Messages.SET_AUTO_COMMIT_ERROR, autoCommit);
			
			throw new DataAccessException(errorMessage, e);
		}
	}
	
	public boolean getAutoCommit() throws DataAccessException
	{
		try
		{
			return this.connection.getAutoCommit();
		}
		catch(SQLException e)
		{
			throw new DataAccessException(e);
		}
	}
	
	/**
	 * Realiza un commit de la conexi&oacute;n.
	 * @throws SQLException
	 * en caso de ocurrir alg&uacute;n error con la base de datos.
	 */
	public void commit() throws SQLException
	{
		this.connection.commit();
	}
	
	/**
	 * Realiza un rollback de la conexi&oacute;n.
	 * @throws SQLException
	 * en caso de ocurrir alg&uacute;n error con la base de datos.
	 */
	public void rollback() throws SQLException
	{
		this.connection.rollback();
	}
	
	public boolean isConnected() throws SQLException
	{
		return this.connection != null && !this.connection.isClosed();
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
		
		DBConnection conexion = (DBConnection)obj;
		
		if(!((this.dbManager == null) ? conexion.dbManager == null : this.dbManager.equals(conexion.dbManager)))
		{
			return false;
		}
		
		if(!((this.database == null) ? conexion.database == null : this.database.equals(conexion.database)))
		{
			return false;
		}
		
		if(!((this.user == null) ? conexion.user == null : this.user.equals(conexion.user)))
		{
			return false;
		}
		
		if(!((this.password == null) ? conexion.password == null : this.password.equals(conexion.password)))
		{
			return false;
		}
		
		if(!((this.host == null) ? conexion.host == null : this.host.equals(conexion.host)))
		{
			return false;
		}
		
		return true;
	}
	
	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = 1;
		result = prime * result	+ ((database == null) ? 0 : database.hashCode());
		result = prime * result + ((dbManager == null) ? 0 : dbManager.hashCode());
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + ((password == null) ? 0 : password.hashCode());
		result = prime * result + ((user == null) ? 0 : user.hashCode());
		
		return result;
	}
	
	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		
		sb.append(this.dbManager).append(" ");
		sb.append(this.database).append("(");
		sb.append(this.host).append(")");
		
		return sb.toString();
	}
}