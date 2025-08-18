package py.com.semp.lib.database.data;

import java.sql.ResultSetMetaData;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import py.com.semp.lib.database.internal.MessageUtil;
import py.com.semp.lib.database.internal.Messages;
import py.com.semp.lib.database.utilities.DatabaseBuilders;
import py.com.semp.lib.utilidades.exceptions.DataAccessException;

/**
 * Shared, immutable schema for a tabular result.
 * <p>
 * Holds column names and Java types by index. Optionally exposes a single
 * {@code tableName} when all reported columns belong to the same table;
 * otherwise {@code tableName} is {@code null} (e.g., joins, expressions).
 * </p>
 */
public final class TypedSchema
{
	private final String[] names;
	private final Class<?>[] types;
	private final Map<String, Integer> nameToIndex;
	private final String tableName;
	
	/**
     * Creates a schema from arrays of names/types and an optional common table name.
     *
     * @param names non-null array of column labels (1:1 with {@code types})
     * @param types non-null array of Java types (1:1 with {@code names})
     * @param tableName common table name if all columns belong to the same table, else {@code null}
     * @throws NullPointerException if {@code names} or {@code types} are null
     * @throws IllegalArgumentException if lengths mismatch
     */
	public TypedSchema(String[] names, Class<?>[] types, String tableName)
	{
		if(names == null || types == null)
		{
			StringBuilder methodName = new StringBuilder();
			
			StringJoiner joiner = new StringJoiner(", ");
			
			if(names == null) joiner.add("names");
			if(types == null) joiner.add("types");
			
			methodName.append("[").append(joiner.toString()).append("] ");
			methodName.append(this.getClass().getSimpleName());
			methodName.append("::");
			methodName.append(this.getClass().getSimpleName());
			methodName.append("(String[] names, Class<?>[] types)");
			
			String errorMessage = MessageUtil.getMessage(Messages.NULL_VALUES_NOT_ALLOWED_ERROR, methodName.toString());
			
			throw new NullPointerException(errorMessage);
		}
		
		if(names.length != types.length)
		{
			throw new IllegalArgumentException("names.length != types.length");
		}
		
		this.names = names.clone();
		this.types = types.clone();
		this.tableName = tableName;
		
		Map<String, Integer> nameToIndex = new HashMap<>(names.length);
		
		for(int i = 0; i < names.length; i++)
		{
			nameToIndex.put(names[i], i);
		}
		
		this.nameToIndex = Collections.unmodifiableMap(nameToIndex);
	}
	
	public TypedSchema(String[] names, Class<?>[] types)
	{
        this(names, types, null);
    }
	
	public static TypedSchema from(ResultSetMetaData meta) throws DataAccessException
	{
		return DatabaseBuilders.getTypedSchema(meta);
	}
	
	public int size()
	{
		return this.names.length;
	}
	
	public String getName(int index)
	{
		return this.names[index];
	}
	
	public Class<?> getType(int index)
	{
		return this.types[index];
	}
	
	public Integer indexOf(String name)
	{
		return this.nameToIndex.get(name);
	}
	
	public String getTableName()
	{
		return this.tableName;
	}
	
	public String[] getNames()
	{
		return this.names.clone();
	}
	
	public Class<?>[] getTypes()
	{
		return this.types.clone();
	}
}