package py.com.semp.lib.database.utilities;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import py.com.semp.lib.utilidades.data.TypedResult;
import py.com.semp.lib.utilidades.data.TypedRow;
import py.com.semp.lib.utilidades.data.TypedValue;

//FIXME revisar
public final class ResultBuilders
{
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
				Class<?> javaType = resolveJavaType(meta, i, value);
				
				// Create TypedValue with explicit type (even if value is null)
				@SuppressWarnings({"unchecked", "rawtypes"})
				TypedValue<?> typedValue = new TypedValue(javaType, value);
				
				row.put(label, typedValue);
			}
			
			rows.add(new TypedRow(row));
		}
		
		return new TypedResult(rows);
	}
	
	private static Class<?> resolveJavaType(ResultSetMetaData meta, int i, Object value) throws SQLException
	{
		// Best: use the driver-exposed Java class name
		try
		{
			String className = meta.getColumnClassName(i);
			
			if(className != null)
			{
				return Class.forName(className);
			}
		}
		catch(ClassNotFoundException ignore)
		{
			/* fallback below */ 
		}
		
		// Fallbacks:
		if(value != null)
		{
			return value.getClass();
		}
		
		// Last resort: map by JDBC type
		switch(meta.getColumnType(i))
		{
			case java.sql.Types.VARCHAR:
			case java.sql.Types.CHAR:
			case java.sql.Types.LONGVARCHAR:
			case java.sql.Types.NVARCHAR:
			case java.sql.Types.NCHAR:
			case java.sql.Types.LONGNVARCHAR:
				return String.class;
			case java.sql.Types.INTEGER:
				return Integer.class;
			case java.sql.Types.BIGINT:
				return Long.class;
			case java.sql.Types.SMALLINT:
				return Short.class;
			case java.sql.Types.TINYINT:
				return Byte.class;
			case java.sql.Types.BOOLEAN:
			case java.sql.Types.BIT:
				return Boolean.class;
			case java.sql.Types.DECIMAL:
			case java.sql.Types.NUMERIC:
				return java.math.BigDecimal.class;
			case java.sql.Types.REAL:
				return Float.class;
			case java.sql.Types.FLOAT:
			case java.sql.Types.DOUBLE:
				return Double.class;
			case java.sql.Types.DATE:
				return java.sql.Date.class;
			case java.sql.Types.TIME:
				return java.sql.Time.class;
			case java.sql.Types.TIMESTAMP:
			case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
				return java.sql.Timestamp.class;
			case java.sql.Types.BINARY:
			case java.sql.Types.VARBINARY:
			case java.sql.Types.LONGVARBINARY:
				return byte[].class;
			default:
				return Object.class;
		}
	}
}
