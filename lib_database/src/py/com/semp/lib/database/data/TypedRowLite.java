package py.com.semp.lib.database.data;

import java.util.StringJoiner;

import py.com.semp.lib.database.internal.MessageUtil;
import py.com.semp.lib.database.internal.Messages;
import py.com.semp.lib.utilidades.exceptions.ObjectNotFoundException;

/**
 * A single row backed by an Object[] plus a shared {@link TypedSchema}.
 * No per-row column name storage.
 */
public final class TypedRowLite
{
	private final TypedSchema schema;
	private final Object[] values;
	
	public TypedRowLite(TypedSchema schema, Object[] values)
	{
		if(schema == null || values == null)
		{
			StringBuilder methodName = new StringBuilder();
			
			StringJoiner joiner = new StringJoiner(", ");
			
			if(schema == null) joiner.add("schema");
			if(values == null) joiner.add("values");
			
			methodName.append("[").append(joiner.toString()).append("] ");
			methodName.append(this.getClass().getSimpleName());
			methodName.append("::");
			methodName.append(this.getClass().getSimpleName());
			methodName.append("(");
			methodName.append(TypedSchema.class.getSimpleName());
			methodName.append("schema, Object[] values)");
			
			String errorMessage = MessageUtil.getMessage(Messages.NULL_VALUES_NOT_ALLOWED_ERROR, methodName.toString());
			
			throw new NullPointerException(errorMessage);
		}
		
		this.schema = schema;
		this.values = values;
		
		if(values.length != schema.size())
		{
			throw new IllegalArgumentException("values.length != schema.size()");
		}
	}
	
	public TypedSchema getSchema()
	{
		return this.schema;
	}
	
	public int size()
	{
		return values.length;
	}
	
	public <T> T get(int index)
	{
		Class<?> type = this.schema.getType(index);
		
		@SuppressWarnings("unchecked")
		T value = (T) type.cast(values[index]);
		
		return value;
	}
	
	public <T> T get(String columnName)
	{
		Integer index = this.schema.indexOf(columnName);
		
		if(index == null)
		{
			String tableName = this.schema.getTableName();
			
			String errorMessage = MessageUtil.getMessage(Messages.FIELD_NOT_FOUND_ERROR, columnName, tableName);
			
			throw new ObjectNotFoundException(errorMessage);
		}
		
		return this.get(index);
	}
	
	public <T> T get(int index, Class<T> type)
	{
		Object value = values[index];
		
		if(value == null)
		{
			return null;
		}
		
		return type.cast(value);
	}
	
	public <T> T get(String columnName, Class<T> type)
	{
		Integer index = this.schema.indexOf(columnName);
		
		if(index == null)
		{
			String tableName = this.schema.getTableName();
			
			String errorMessage = MessageUtil.getMessage(Messages.FIELD_NOT_FOUND_ERROR, columnName, tableName);
			
			throw new ObjectNotFoundException(errorMessage);
		}
		
		Object value = values[index];
		
		if(value == null)
		{
			return null;
		}
		
		return type.cast(value);
	}
	
	@Override
	public String toString()
	{
		StringJoiner j = new StringJoiner(" | ");
		for(Object v : values) j.add(String.valueOf(v));
		return j.toString();
	}
}
