package py.com.semp.lib.database.utilities;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import py.com.semp.lib.database.data.TypedSchema;
import py.com.semp.lib.database.internal.MessageUtil;
import py.com.semp.lib.database.internal.Messages;
import py.com.semp.lib.utilidades.data.TypedResult;
import py.com.semp.lib.utilidades.data.TypedRow;
import py.com.semp.lib.utilidades.data.TypedValue;
import py.com.semp.lib.utilidades.exceptions.DataAccessException;

//FIXME revisar

/**
 * Builders/utilities for converting JDBC results and metadata to typed structures.
 */
public final class DatabaseBuilders
{
	private static final ConcurrentMap<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();
	
	private DatabaseBuilders()
	{
		String errorMessage = MessageUtil.getMessage(Messages.DONT_INSTANTIATE, DatabaseBuilders.class.getName());
		
		throw new AssertionError(errorMessage);
	}
	
	public static TypedResult toTypedResult(ResultSet resultSet) throws SQLException
	{
		ResultSetMetaData meta = resultSet.getMetaData();
		
		int columnCount = meta.getColumnCount();
		
		List<TypedRow> rows = new ArrayList<>();
		
		while(resultSet.next())
		{
			Map<String, TypedValue<?>> row = new LinkedHashMap<>();
			
			for(int i = 1; i <= columnCount; i++)
			{
				String label = meta.getColumnLabel(i);
				Object value = resultSet.getObject(i);
				Class<?> javaType = resolveJavaType(meta, i);
				
				// Create TypedValue with explicit type (even if value is null)
				@SuppressWarnings({"unchecked", "rawtypes"})
				TypedValue<?> typedValue = new TypedValue(javaType, value);
				
				row.put(label, typedValue);
			}
			
			rows.add(new TypedRow(row));
		}
		
		return new TypedResult(rows);
	}
	
	/**
     * Resolves the best Java class for the column at index {@code i}, using driver hints
     * and falling back to a JDBC class mapping.
     */
	public static Class<?> resolveJavaType(ResultSetMetaData meta, int i) throws SQLException
	{
		Objects.requireNonNull(meta, "meta");
		
		// Use the driver-exposed Java class name
		try
		{
			String className = meta.getColumnClassName(i);
			
			if(className != null && !Object.class.getName().equals(className))
			{
				// cache to avoid repeated Class.forName on hot paths
				return CLASS_CACHE.computeIfAbsent(className, DatabaseBuilders::forNameQuiet);
			}
		}
		catch(SQLException ignore)
		{
		}
		
		// fall back: Map by JDBC type
		switch(meta.getColumnType(i))
		{
			case java.sql.Types.VARCHAR:
			case java.sql.Types.CHAR:
			case java.sql.Types.LONGVARCHAR:
			case java.sql.Types.NVARCHAR:
			case java.sql.Types.NCHAR:
			case java.sql.Types.LONGNVARCHAR: return String.class;
			case java.sql.Types.INTEGER: return Integer.class;
			case java.sql.Types.BIGINT: return Long.class;
			case java.sql.Types.SMALLINT: return Short.class;
			case java.sql.Types.TINYINT: return Byte.class;
			case java.sql.Types.BOOLEAN:
			case java.sql.Types.BIT: return Boolean.class;
			case java.sql.Types.DECIMAL:
			case java.sql.Types.NUMERIC: return java.math.BigDecimal.class;
			case java.sql.Types.REAL: return Float.class;
			case java.sql.Types.FLOAT:
			case java.sql.Types.DOUBLE: return Double.class;
			case java.sql.Types.DATE: return java.sql.Date.class;
			case java.sql.Types.TIME: return java.sql.Time.class;
			case java.sql.Types.TIMESTAMP:
			case java.sql.Types.TIMESTAMP_WITH_TIMEZONE: return java.sql.Timestamp.class;
			case java.sql.Types.BINARY:
			case java.sql.Types.VARBINARY:
			case java.sql.Types.LONGVARBINARY: return byte[].class;
			default: return Object.class;
		}
	}
	
	private static Class<?> forNameQuiet(String className)
	{
		try
		{
			return Class.forName(className);
		}
		catch(ClassNotFoundException e)
		{
			return Object.class;
		}
	}
	
	 /**
     * Builds a {@link TypedSchema} from {@link ResultSetMetaData}.
     * <p>
     * {@code tableName} is populated only when <em>all</em> columns report the <em>same non-empty</em>
     * name via {@code meta.getTableName(i)}. If any column is blank or mismatched (e.g., joins, expressions),
     * the schema's {@code tableName} is {@code null}.
     */
	public static TypedSchema getTypedSchema(ResultSetMetaData meta) throws DataAccessException
	{
		try
		{
			int columnCount = meta.getColumnCount();
			String[] names = new String[columnCount];
			Class<?>[] types = new Class<?>[columnCount];
			String commonTableName = null;
			
			boolean invalidTableName = false;
			
			for(int i = 1; i <= columnCount; i++)
			{
				String label = meta.getColumnLabel(i);
				
				if(label == null || label.isEmpty())
				{
					label = meta.getColumnName(i);
				}
				
				names[i - 1] = label;
				types[i - 1] = resolveJavaType(meta, i);
				
				String tableName = meta.getTableName(i);
				
				if(tableName == null || tableName.isEmpty())
				{
					invalidTableName = true;
					commonTableName = null;
				}
				
				if(!invalidTableName)
				{
					if(commonTableName == null)
					{
						commonTableName = tableName;
					}
					else if(!commonTableName.equals(tableName))
					{
						commonTableName = null;
						invalidTableName = true;
					}
				}
			}
			
			return new TypedSchema(names, types, commonTableName);
		}
		catch (SQLException e)
		{
			String errorMessage = MessageUtil.getMessage(Messages.UNABLE_T0_OBTAIN_METADATA_ERROR);
			
			throw new DataAccessException(errorMessage, e);
		}
	}
}
