package py.com.semp.lib.database.utilities;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import py.com.semp.lib.database.internal.MessageUtil;
import py.com.semp.lib.database.internal.Messages;
import py.com.semp.lib.utilidades.exceptions.DataAccessException;

/**
 * Utility class providing common helper methods for JDBC operations.
 * 
 * <p>This class is not meant to be instantiated.</p>
 * 
 * @author Sergio Morel
 */
public final class DBUtils
{
	public static final String NAMED_QUERY_REGEX = "(:+)([a-zA-Z_][a-zA-Z0-9_]*)";
	public static final Pattern NAMED_QUERY_PATTERN = Pattern.compile(NAMED_QUERY_REGEX);
	
	private DBUtils()
	{
		String errorMessage = MessageUtil.getMessage(Messages.DONT_INSTANTIATE, this.getClass().getName());
		
		throw new AssertionError(errorMessage);
	}
	
	/**
	 * Converts a SQL string with named parameters to a JDBC-compatible SQL using '?' placeholders.
	 * Names like :name are replaced by ? and escaped colons (::) are preserved as :.
	 *
	 * @param namedQuery The original SQL with named parameters.
	 * @param valuesMap A map containing values for each parameter.
	 * @param values Output list where parameters will be stored in positional order.
	 * @return A JDBC-compatible SQL string with '?' placeholders.
	 * @throws DataAccessException If a parameter in the query is missing from the values map.
	 * @throws IllegalArgumentException if any argument is null.
	 */
	
	/**
     * Converts a SQL query containing named parameters into a JDBC-compatible query
     * using '?' placeholders. The values for each named parameter are extracted from
     * the provided {@code valuesMap} and stored in {@code values} in the order they appear.
     * <p>
     * This method also supports colon escaping. A parameter can be escaped by prefixing
     * it with an extra colon:
     * <ul>
     *   <li>{@code :name} is replaced with {@code ?}</li>
     *   <li>{@code ::name} is preserved as {@code :name}</li>
     *   <li>{@code :::name} becomes {@code :?}</li>
     * </ul>
     * </p>
     *
     * @param namedQuery The SQL query string containing named parameters (e.g., {@code :param}).
     * @param valuesMap A map from parameter names to their corresponding values.
     * @param values Output list that will be populated with values in positional order
     *               matching the final JDBC query.
     * @return A JDBC-ready query string with all named parameters replaced by '?' as needed.
     *
     * @throws IllegalArgumentException if any of the arguments are {@code null}.
     * @throws DataAccessException if the query contains a named parameter not present in {@code valuesMap}.
     */
	public static String parseNamedQueryToJDBC(final String namedQuery, final Map<String, Object> valuesMap, final List<Object> values) throws DataAccessException
	{
		if (namedQuery == null || valuesMap == null || values == null)
		{
			String errorMessage = MessageUtil.getMessage(Messages.NULL_ARGUMENT_ERROR);
			
			throw new IllegalArgumentException(errorMessage);
		}
		
		Matcher matcher = NAMED_QUERY_PATTERN.matcher(namedQuery);
		
		StringBuffer jdbcSQL = new StringBuffer();
		
		while(matcher.find())
		{
			String colons = matcher.group(1);
			String name = matcher.group(2);
			
			StringBuilder replacement = new StringBuilder();
			
			if(colons.length() % 2 == 0)
			{
				replacement.append(":".repeat(colons.length() / 2));
				replacement.append(name);
			}
			else
			{
				if(!valuesMap.containsKey(name))
				{
					String errorMessage = MessageUtil.getMessage(Messages.MISSING_VALUE_ERROR, name);
					
					throw new DataAccessException(errorMessage);
				}
				
				replacement.append(":".repeat((colons.length() - 1) / 2));
				replacement.append("?");
				
				values.add(valuesMap.get(name));
			}
			
			matcher.appendReplacement(jdbcSQL, Matcher.quoteReplacement(replacement.toString()));
		}
		
		matcher.appendTail(jdbcSQL);
		
		return jdbcSQL.toString();
	}
}