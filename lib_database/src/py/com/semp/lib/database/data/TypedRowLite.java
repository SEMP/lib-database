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
	
	public Object get(int index)
	{
		return values[index];
	}
	
	public Object get(String column)
	{
		Integer index = this.schema.indexOf(column);
		
		if(index == null)
		{
			String errorMessage = MessageUtil.getMessage(Messages.FIELD_NOT_FOUND_ERROR, column, tableName);
			throw new ObjectNotFoundException(errorMessage);
		}
		
		return values[index];
	}
	
	public <T> T get(int index, Class<T> type)
	{
		Object v = values[index];
		return (v == null) ? null : type.cast(v);
	}
	
	public <T> T get(String column, Class<T> type)
	{
		Integer idx = schema.indexOf(column);
		if(idx == null) return null;
		Object v = values[idx];
		return (v == null) ? null : type.cast(v);
	}
	
	@Override
	public String toString()
	{
		StringJoiner j = new StringJoiner(" | ");
		for(Object v : values) j.add(String.valueOf(v));
		return j.toString();
	}
}
