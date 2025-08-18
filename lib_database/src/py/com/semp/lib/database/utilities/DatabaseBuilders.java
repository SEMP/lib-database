package py.com.semp.lib.database.utilities;

import java.lang.reflect.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import py.com.semp.lib.database.configuration.Values;
import py.com.semp.lib.database.data.TypedRowLite;
import py.com.semp.lib.database.data.TypedSchema;
import py.com.semp.lib.database.internal.MessageUtil;
import py.com.semp.lib.database.internal.Messages;
import py.com.semp.lib.utilidades.exceptions.DataAccessException;
import py.com.semp.lib.utilidades.log.Logger;
import py.com.semp.lib.utilidades.log.LoggerManager;

/**
 * Builders/utilities for converting JDBC results and metadata to typed structures.
 */
public final class DatabaseBuilders
{
	private static final ConcurrentMap<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();
	
	private static final Map<Class<?>, Class<?>> PRIMITIVE_TO_WRAPPER = new HashMap<>();
	
	static
	{
		PRIMITIVE_TO_WRAPPER.put(boolean.class, Boolean.class);
		PRIMITIVE_TO_WRAPPER.put(byte.class, Byte.class);
		PRIMITIVE_TO_WRAPPER.put(short.class, Short.class);
		PRIMITIVE_TO_WRAPPER.put(char.class, Character.class);
		PRIMITIVE_TO_WRAPPER.put(int.class, Integer.class);
		PRIMITIVE_TO_WRAPPER.put(long.class, Long.class);
		PRIMITIVE_TO_WRAPPER.put(float.class, Float.class);
		PRIMITIVE_TO_WRAPPER.put(double.class, Double.class);
	}
	
	private DatabaseBuilders()
	{
		String errorMessage = MessageUtil.getMessage(Messages.DONT_INSTANTIATE, DatabaseBuilders.class.getName());
		
		throw new AssertionError(errorMessage);
	}
	
//	//FIXME revisar
//	public static TypedResult toTypedResult(ResultSet resultSet) throws SQLException
//	{
//		ResultSetMetaData meta = resultSet.getMetaData();
//		
//		int columnCount = meta.getColumnCount();
//		
//		List<TypedRow> rows = new ArrayList<>();
//		
//		while(resultSet.next())
//		{
//			Map<String, TypedValue<?>> row = new LinkedHashMap<>();
//			
//			for(int i = 1; i <= columnCount; i++)
//			{
//				String label = meta.getColumnLabel(i);
//				Object value = resultSet.getObject(i);
//				Class<?> javaType = resolveJavaType(meta, i);
//				
//				// Create TypedValue with explicit type (even if value is null)
//				@SuppressWarnings({"unchecked", "rawtypes"})
//				TypedValue<?> typedValue = new TypedValue(javaType, value);
//				
//				row.put(label, typedValue);
//			}
//			
//			rows.add(new TypedRow(row));
//		}
//		
//		return new TypedResult(rows);
//	}
	
	/**
     * Resolves the best Java class for the column at index {@code i}, using driver hints
     * and falling back to a JDBC class mapping.
     */
	public static Class<?> resolveJavaType(ResultSetMetaData metadata, int i)
	{
		Objects.requireNonNull(metadata, "metadata");
		
		// Use the driver-exposed Java class name
		try
		{
			String className = metadata.getColumnClassName(i);
			
			if(className != null && !Object.class.getName().equals(className))
			{
				// cache to avoid repeated Class.forName on hot paths
				return CLASS_CACHE.computeIfAbsent(className, DatabaseBuilders::forNameQuiet);
			}
		}
		catch(SQLException ignore)
		{
			Logger logger = LoggerManager.getLogger(Values.Constants.DATABASE_CONTEXT);
			
			logger.debug(ignore);
		}
		
		try
		{
			// fall back: Map by JDBC type
			switch(metadata.getColumnType(i))
			{
				case java.sql.Types.VARCHAR:
				case java.sql.Types.CHAR:
				case java.sql.Types.LONGVARCHAR:
				case java.sql.Types.NVARCHAR:
				case java.sql.Types.NCHAR:
				case java.sql.Types.LONGNVARCHAR:				return String.class;
				case java.sql.Types.INTEGER:					return Integer.class;
				case java.sql.Types.BIGINT:						return Long.class;
				case java.sql.Types.SMALLINT:					return Short.class;
				case java.sql.Types.TINYINT:					return Byte.class;
				case java.sql.Types.BOOLEAN:
				case java.sql.Types.BIT:						return Boolean.class;
				case java.sql.Types.DECIMAL:
				case java.sql.Types.NUMERIC:					return java.math.BigDecimal.class;
				case java.sql.Types.REAL:						return Float.class;
				case java.sql.Types.FLOAT:
				case java.sql.Types.DOUBLE:						return Double.class;
				case java.sql.Types.DATE:						return java.sql.Date.class;
				case java.sql.Types.TIME:						return java.sql.Time.class;
				case java.sql.Types.TIMESTAMP:					return java.sql.Timestamp.class;
				case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:	return java.time.OffsetDateTime.class;
				case java.sql.Types.TIME_WITH_TIMEZONE:			return java.time.OffsetTime.class;
				case java.sql.Types.BINARY:
				case java.sql.Types.VARBINARY:
				case java.sql.Types.LONGVARBINARY:				return byte[].class;
				case java.sql.Types.ARRAY:						return Object[].class;
				case java.sql.Types.SQLXML:						return String.class;
			    case java.sql.Types.CLOB:
			    case java.sql.Types.NCLOB:						return String.class;
			    case java.sql.Types.BLOB:						return byte[].class;
				default:										return Object.class;
			}
		}
		catch(SQLException e)
		{
			Logger logger = LoggerManager.getLogger(Values.Constants.DATABASE_CONTEXT);
			
			String debugMessage = MessageUtil.getMessage(Messages.FIELD_TYPE_FALLBACK_OBJECT, i);
			
			logger.debug(debugMessage, e);
			
			return Object.class;
		}
	}
	
	private static Class<?> forNameQuiet(String className)
	{
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		
		if(classLoader == null)
		{
			classLoader = DatabaseBuilders.class.getClassLoader();
		}
		
		try
		{
			return Class.forName(className, false, classLoader);
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
	public static TypedSchema getTypedSchema(ResultSetMetaData metadata) throws DataAccessException
	{
		try
		{
			int columnCount = metadata.getColumnCount();
			String[] names = new String[columnCount];
			Class<?>[] types = new Class<?>[columnCount];
			String commonTableName = null;
			
			boolean invalidTableName = false;
			
			for(int i = 1; i <= columnCount; i++)
			{
				String label = metadata.getColumnLabel(i);
				
				if(label == null || label.isEmpty())
				{
					label = metadata.getColumnName(i);
				}
				
				names[i - 1] = label;
				types[i - 1] = resolveJavaType(metadata, i);
				
				String tableName = metadata.getTableName(i);
				
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
		catch(SQLException e)
		{
			String errorMessage = MessageUtil.getMessage(Messages.UNABLE_TO_OBTAIN_METADATA_ERROR);
			
			throw new DataAccessException(errorMessage, e);
		}
	}
	
	public static TypedRowLite toTypedRowLite(ResultSet resultSet) throws DataAccessException
	{
		Objects.requireNonNull(resultSet, "resultSet");
		
		try
		{
			ResultSetMetaData metadata = resultSet.getMetaData();
			
			Objects.requireNonNull(metadata, "resultSet.getMetaData()");
			
			int columnCount = metadata.getColumnCount();
			
			TypedSchema schema = TypedSchema.from(metadata);
			
			Object[] values = new Object[columnCount];
			
			for(int i = 1; i <= columnCount; i++)
			{
				int jdbcType = metadata.getColumnType(i);
				
				Class<?> type = resolveJavaType(metadata, i);
				
				values[i - 1] = readValue(resultSet, i, type, jdbcType);
			}
			
			return new TypedRowLite(schema, values);
		}
		catch(SQLException e)
		{
			String errorMessage = MessageUtil.getMessage(Messages.UNABLE_TO_OBTAIN_METADATA_ERROR);
			
			throw new DataAccessException(errorMessage, e);
		}
	}
	
	public static Object readValue(ResultSet resultSet, int i, Class<?> resolvedType, int jdbcType) throws DataAccessException
	{
		try
		{
			if(resolvedType == java.time.OffsetDateTime.class)
			{
				try
				{
					return resultSet.getObject(i, java.time.OffsetDateTime.class);
				}
				catch(AbstractMethodError | SQLException e)
				{
					java.sql.Timestamp timeStamp = resultSet.getTimestamp(i);
					
					if(timeStamp == null)
					{
						return null;
					}
					
					return timeStamp.toInstant().atOffset(java.time.ZoneOffset.UTC);
				}
			}
			
			if(resolvedType == Object[].class)
			{
				java.sql.Array array = resultSet.getArray(i);
				
				if(array == null)
				{
					return null;
				}
				
				try
				{
					return materializeArray(array);
				}
				finally
				{
					try
					{
						array.free();
					}
					catch(SQLException ignore)
					{
						Logger logger = LoggerManager.getLogger(Values.Constants.DATABASE_CONTEXT);
						
						logger.debug(ignore);
					}
				}
			}
			
			if(resolvedType == java.time.OffsetTime.class)
			{
				try
				{
					return resultSet.getObject(i, java.time.OffsetTime.class);
				}
				catch(AbstractMethodError | SQLException e)
				{
					java.sql.Time time = resultSet.getTime(i);
					
					if(time == null)
					{
						return null;
					}
					
					return time.toLocalTime().atOffset(java.time.ZoneOffset.UTC);
				}
			}
			
			if(resolvedType == String.class && jdbcType == java.sql.Types.SQLXML)
			{
				java.sql.SQLXML xml = resultSet.getSQLXML(i);
				
				if(xml == null)
				{
					return null;
				}
				
				try
				{
					return xml.getString();
				}
				finally
				{
					try
					{
						xml.free();
					}
					catch(SQLException ignore)
					{
						Logger logger = LoggerManager.getLogger(Values.Constants.DATABASE_CONTEXT);
						
						logger.debug(ignore);
					}
				}
			}
			
			if(resolvedType == String.class && (jdbcType == java.sql.Types.CLOB || jdbcType == java.sql.Types.NCLOB))
			{
				java.sql.Clob clob = resultSet.getClob(i);
				
				if(clob == null)
				{
					return null;
				}
				
				try
				{
					return readClobFully(clob);
				}
				finally
				{
					try
					{
						clob.free();
					}
					catch(SQLException ignore)
					{
						Logger logger = LoggerManager.getLogger(Values.Constants.DATABASE_CONTEXT);
						
						logger.debug(ignore);
					}
				}
			}
			
			if(resolvedType == byte[].class && jdbcType == java.sql.Types.BLOB)
			{
				java.sql.Blob blob = resultSet.getBlob(i);
				
				if(blob == null)
				{
					return null;
				}
				
				try
				{
					return readBlobFully(blob);
				}
				finally
				{
					try
					{
						blob.free();
					}
					catch(SQLException ignore)
					{
						Logger logger = LoggerManager.getLogger(Values.Constants.DATABASE_CONTEXT);
						
						logger.debug(ignore);
					}
				}
			}
			
			if(resolvedType == Object.class && jdbcType == java.sql.Types.ARRAY)
			{
				java.sql.Array array = resultSet.getArray(i);
				
				if(array == null)
				{
					return null;
				}
				
				try
				{
					return materializeArray(array);
				}
				finally
				{
					try
					{
						array.free();
					}
					catch(SQLException ignore)
					{
						Logger logger = LoggerManager.getLogger(Values.Constants.DATABASE_CONTEXT);
						
						logger.debug(ignore);
					}
				}
			}
			
			return resultSet.getObject(i);
		}
		catch(SQLException e)
		{
			throw new DataAccessException(e);
		}
	}
	
	private static Object boxPrimitiveArray(Object primitiveArray)
	{
		Class<?> primitiveComponent = primitiveArray.getClass().getComponentType();
		
		Class<?> wrapperComponent = PRIMITIVE_TO_WRAPPER.get(primitiveComponent);
		
		if(wrapperComponent == null)
		{
			return primitiveArray;
		}
		
		int length = Array.getLength(primitiveArray);
		
		Object boxed = Array.newInstance(wrapperComponent, length);
		
		for(int i = 0; i < length; i++)
		{
			Array.set(boxed, i, Array.get(primitiveArray, i));
		}
		
		return boxed;
	}
	
	private static Object materializeArray(java.sql.Array array) throws SQLException
	{
		Object raw = array.getArray();
		
		if(raw == null)
		{
			return null;
		}
		
		Class<?> rawClass = raw.getClass();
		
		if(!rawClass.isArray())
		{
			return raw;
		}
		
		Class<?> component = rawClass.getComponentType();
		
		if(component.isPrimitive())
		{
			return boxPrimitiveArray(raw);
		}
		
		return raw;
	}
	
	private static String readClobFully(java.sql.Clob clob) throws SQLException
	{
		long length = clob.length();
		
		if(length > Integer.MAX_VALUE)
		{
			String errorMessage = MessageUtil.getMessage(Messages.TOO_LARGE_TO_MATERIALIZE_ERROR, "CLOB", length);
			
			throw new SQLException(errorMessage);
		}
		
		return clob.getSubString(1, (int)length);
	}
	
	private static byte[] readBlobFully(java.sql.Blob blob) throws SQLException
	{
		long length = blob.length();
		
		if(length > Integer.MAX_VALUE)
		{
			String errorMessage = MessageUtil.getMessage(Messages.TOO_LARGE_TO_MATERIALIZE_ERROR, "BLOB", length);
			
			throw new SQLException(errorMessage);
		}
		
		return blob.getBytes(1, (int)length);
	}
}
