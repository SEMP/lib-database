package py.com.semp.lib.database.data;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import py.com.semp.lib.database.internal.MessageUtil;
import py.com.semp.lib.database.internal.Messages;

/**
 * Shared, immutable schema for a tabular result.
 * Holds column names and Java types by index.
 */
public final class TypedSchema
{
	private final String[] names;
	private final Class<?>[] types;
	private final Map<String, Integer> nameToIndex;
	
	public TypedSchema(String[] names, Class<?>[] types)
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
		
		Map<String, Integer> nameToIndex = new HashMap<>(names.length);
		
		for(int i = 0; i < names.length; i++)
		{
			nameToIndex.put(names[i], i);
		}
		
		this.nameToIndex = Collections.unmodifiableMap(nameToIndex);
	}
	
	public int size()
	{
		return names.length;
	}
	
	public String getName(int index)
	{
		return names[index];
	}
	
	public Class<?> getType(int index)
	{
		return types[index];
	}
	
	public Integer indexOf(String name)
	{
		return nameToIndex.get(name);
	}
	
	public String[] getNames()
	{
		return names.clone();
	}
	
	public Class<?>[] getTypes()
	{
		return types.clone();
	}
}
