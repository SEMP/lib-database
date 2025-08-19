package py.com.semp.lib.database.utilities;

import java.lang.reflect.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
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
	private static final int MAX_SIZE_LIMIT = Integer.MAX_VALUE;
	
	private static final ConcurrentMap<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();
	
	private static final Map<Class<?>, Class<?>> PRIMITIVE_TO_WRAPPER = new HashMap<>();
	
	private static final ThreadLocal<Calendar> UTC_CALENDAR = ThreadLocal.withInitial(() -> Calendar.getInstance(TimeZone.getTimeZone("UTC")));
	
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
	
	/**
     * Resolves the best Java class for the column at index {@code i}, using driver hints
     * and falling back to a JDBC class mapping.
     * 
	 * Note: LOB columns (CLOB/NCLOB/BLOB) and SQLXML are fully materialized
	 * into String/byte[]/String respectively. For very large data, prefer
	 * streaming via JDBC APIs instead of using this helper.
	 *
	 * For TIMESTAMP WITH TIME ZONE / TIME WITH TIME ZONE, this attempts
	 * to return OffsetDateTime/OffsetTime; if the driver doesn't support the
	 * typed getters, values are converted assuming UTC.
	 */
	public static Class<?> resolveJavaType(ResultSetMetaData metadata, int i)
	{
		Objects.requireNonNull(metadata, "metadata");
		
		try
		{
			String className = metadata.getColumnClassName(i);
			
			if(className != null && !className.isBlank() && !Object.class.getName().equals(className))
			{
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
				case java.sql.Types.DATE:						return java.time.LocalDate.class;
				case java.sql.Types.TIME:						return java.time.LocalTime.class;
				case java.sql.Types.TIMESTAMP:					return java.time.LocalDateTime.class;
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
			    case java.sql.Types.ROWID:						return java.sql.RowId.class;
			    case java.sql.Types.DATALINK:					return java.net.URL.class;
				case java.sql.Types.OTHER:						return getOtherType(metadata, i);
				case java.sql.Types.DISTINCT:
				case java.sql.Types.NULL:
				case java.sql.Types.JAVA_OBJECT:
				case java.sql.Types.STRUCT:
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
	
	private static Class<?> getOtherType(ResultSetMetaData metadata, int i)
	{
		try
		{
			String typeName = metadata.getColumnTypeName(i);
			
			if("uuid".equalsIgnoreCase(typeName))
			{
				return java.util.UUID.class;
			}
			
			if("json".equalsIgnoreCase(typeName) || "jsonb".equalsIgnoreCase(typeName))
			{
				return String.class;
			}
		}
		catch(SQLException ignore)
		{
			Logger logger = LoggerManager.getLogger(Values.Constants.DATABASE_CONTEXT);
			
			logger.debug(ignore);
		}
		
		return Object.class;
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
			if(resolvedType == String.class)
			{
				if(jdbcType == java.sql.Types.SQLXML)
				{
					return getXMLString(resultSet, i);
				}
				else if(jdbcType == java.sql.Types.CLOB || jdbcType == java.sql.Types.NCLOB)
				{
					return getClobString(resultSet, i);
				}
				
				return resultSet.getString(i);
			}
			else if(resolvedType == byte[].class)
			{
				boolean binaryArrayType = 
					jdbcType == java.sql.Types.BINARY
					|| jdbcType == java.sql.Types.VARBINARY
					|| jdbcType == java.sql.Types.LONGVARBINARY;
				
				if(binaryArrayType)
				{
					return resultSet.getBytes(i);
				}
				else if(jdbcType == java.sql.Types.BLOB)
				{
					return getBlobByteArray(resultSet, i);
				}
			}
			else if(resolvedType == java.time.OffsetDateTime.class)
			{
				return getOffsetDateTime(resultSet, i);
			}
			else if(resolvedType == java.time.OffsetTime.class)
			{
				return getOffsetTime(resultSet, i);
			}
			else if(resolvedType == java.time.LocalDate.class)
			{
				return getLocalDate(resultSet, i);
			}
			else if(resolvedType == java.time.LocalTime.class)
			{
				return getLocalTime(resultSet, i);
			}
			else if(resolvedType == java.time.LocalDateTime.class)
			{
				return getLocalDateTime(resultSet, i);
			}
			else if(resolvedType == Object[].class)
			{
				return getObjectArray(resultSet, i);
			}
			else if(resolvedType == java.util.UUID.class)
			{
				return getUUID(resultSet, i);
			}
			else if(resolvedType == java.sql.RowId.class)
			{
				return resultSet.getRowId(i);
			}
			else if(resolvedType == java.net.URL.class)
			{
				return resultSet.getURL(i);
			}
			else if(resolvedType == Object.class && jdbcType == java.sql.Types.ARRAY)
			{
				return getArray(resultSet, i);
			}
			
			return resultSet.getObject(i);
		}
		catch(SQLException e)
		{
			throw new DataAccessException(e);
		}
	}
	
	private static Object getArray(ResultSet resultSet, int i) throws SQLException
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
	
	private static java.util.UUID getUUID(ResultSet resultSet, int i) throws SQLException
	{
		try
		{
			return resultSet.getObject(i, java.util.UUID.class);
		}
		catch(AbstractMethodError | SQLException ignore)
		{
			Logger logger = LoggerManager.getLogger(Values.Constants.DATABASE_CONTEXT);
			
			logger.debug(ignore);
		}
		
		Object uuid = resultSet.getObject(i);
		
		if(uuid == null)
		{
			return null;
		}
		
		if(uuid instanceof java.util.UUID)
		{
			return (java.util.UUID) uuid;
		}
		
		return java.util.UUID.fromString(uuid.toString());
	}
	
	private static byte[] getBlobByteArray(ResultSet resultSet, int i) throws SQLException
	{
		java.sql.Blob blob = resultSet.getBlob(i);
		
		try
		{
			return readBlobFully(blob);
		}
		finally
		{
			try
			{
				if(blob != null) blob.free();
			}
			catch(SQLException ignore)
			{
				Logger logger = LoggerManager.getLogger(Values.Constants.DATABASE_CONTEXT);
				
				logger.debug(ignore);
			}
		}
	}
	
	private static String getClobString(ResultSet resultSet, int i) throws SQLException
	{
		java.sql.Clob clob = resultSet.getClob(i);
		
		try
		{
			return readClobFully(clob);
		}
		finally
		{
			try
			{
				if(clob != null) clob.free();
			}
			catch(SQLException ignore)
			{
				Logger logger = LoggerManager.getLogger(Values.Constants.DATABASE_CONTEXT);
				
				logger.debug(ignore);
			}
		}
	}
	
	private static String getXMLString(ResultSet resultSet, int i) throws SQLException
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
	
	private static Object[] getObjectArray(ResultSet resultSet, int i) throws SQLException
	{
		java.sql.Array array = resultSet.getArray(i);
		
		try
		{
			return (Object[]) materializeArray(array);
		}
		finally
		{
			try
			{
				if(array != null) array.free();
			}
			catch(SQLException ignore)
			{
				Logger logger = LoggerManager.getLogger(Values.Constants.DATABASE_CONTEXT);
				
				logger.debug(ignore);
			}
		}
	}
	
	private static java.time.LocalDateTime getLocalDateTime(ResultSet resultSet, int i) throws SQLException
	{
		try
		{
			return resultSet.getObject(i, java.time.LocalDateTime.class);
		}
		catch(AbstractMethodError | SQLException e)
		{
			java.sql.Timestamp timeStamp = resultSet.getTimestamp(i);
			
			if(timeStamp == null)
			{
				return null;
			}
			
			return timeStamp.toLocalDateTime();
		}
	}
	
	private static java.time.LocalTime getLocalTime(ResultSet resultSet, int i) throws SQLException
	{
		try
		{
			return resultSet.getObject(i, java.time.LocalTime.class);
		}
		catch(AbstractMethodError | SQLException e)
		{
			java.sql.Time time = resultSet.getTime(i);
			
			if(time == null)
			{
				return null;
			}
			
			return time.toLocalTime();
		}
	}
	
	private static java.time.LocalDate getLocalDate(ResultSet resultSet, int i) throws SQLException
	{
		try
		{
			return resultSet.getObject(i, java.time.LocalDate.class);
		}
		catch(AbstractMethodError | SQLException e)
		{
			java.sql.Date date = resultSet.getDate(i);
			
			if(date == null)
			{
				return null;
			}
			
			return date.toLocalDate();
		}
	}
	
	private static java.time.OffsetTime getOffsetTime(ResultSet resultSet, int i) throws SQLException
	{
		try
		{
			return resultSet.getObject(i, java.time.OffsetTime.class);
		}
		catch(AbstractMethodError | SQLException e)
		{
			String timeString = resultSet.getString(i);
			
			if(timeString != null)
			{
				try
				{
					return java.time.OffsetTime.parse(timeString);
				}
				catch(DateTimeParseException ignore)
				{
					Logger logger = LoggerManager.getLogger(Values.Constants.DATABASE_CONTEXT);
					
					logger.debug(ignore);
				}
			}
			
			java.sql.Time time = resultSet.getTime(i, UTC_CALENDAR.get());
			
			if(time == null)
			{
				return null;
			}
			
			return time.toLocalTime().atOffset(java.time.ZoneOffset.UTC);
		}
	}
	
	private static java.time.OffsetDateTime getOffsetDateTime(ResultSet resultSet, int i) throws SQLException
	{
		try
		{
			return resultSet.getObject(i, java.time.OffsetDateTime.class);
		}
		catch(AbstractMethodError | SQLException e)
		{
			String dateString = resultSet.getString(i);
			
			if(dateString != null)
			{
				try
				{
					return java.time.OffsetDateTime.parse(dateString);
				}
				catch(DateTimeParseException ignore)
				{
					Logger logger = LoggerManager.getLogger(Values.Constants.DATABASE_CONTEXT);
					
					logger.debug(ignore);
				}
			}
			
			java.sql.Timestamp timeStamp = resultSet.getTimestamp(i, UTC_CALENDAR.get());
			
			if(timeStamp == null)
			{
				return null;
			}
			
			return timeStamp.toInstant().atOffset(java.time.ZoneOffset.UTC);
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
		if(array == null)
		{
			return null;
		}
		
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
		if(clob == null)
		{
			return null;
		}
		
		long length = clob.length();
		
		if(length > MAX_SIZE_LIMIT)
		{
			String errorMessage = MessageUtil.getMessage(Messages.TOO_LARGE_TO_MATERIALIZE_ERROR, "CLOB", length);
			
			throw new SQLException(errorMessage);
		}
		
		return clob.getSubString(1, (int)length);
	}
	
	private static byte[] readBlobFully(java.sql.Blob blob) throws SQLException
	{
		if(blob == null)
		{
			return null;
		}
		
		long length = blob.length();
		
		if(length > MAX_SIZE_LIMIT)
		{
			String errorMessage = MessageUtil.getMessage(Messages.TOO_LARGE_TO_MATERIALIZE_ERROR, "BLOB", length);
			
			throw new SQLException(errorMessage);
		}
		
		return blob.getBytes(1, (int)length);
	}
}