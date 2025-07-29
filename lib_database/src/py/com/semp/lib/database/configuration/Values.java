package py.com.semp.lib.database.configuration;

import py.com.semp.lib.database.connection.DatabaseEngine;

/**
 * Contains the value of constants used in the project.
 * 
 * @author Sergio Morel
 */
public interface Values
{
	/**
	 * Contains constant values
	 * 
	 * @author Sergio Morel
	 */
	public interface Constants
	{
		//String values
		
		/**
		 * Context name for the socket library
		 */
		public static final String DATABASE_CONTEXT = "lib_database";
		
		/**
		 * Path where the messages for localization are found.
		 */
		public static final String MESSAGES_PATH = "/py/com/semp/lib/database/";
	}
	
	/**
	 * Contains variable names
	 * 
	 * @author Sergio Morel
	 */
	public interface VariableNames
	{
		//Integer values
		
		/**
		 * Time out time in seconds for establishing connections.
		 */
		public static final String CONNECTION_TIMEOUT_SECONDS = "connectionTimeoutSeconds";
		
		// String Variables Names
		
		/**
		 * The user used to connect to the database.
		 */
		public static final String DATABASE_USER_NAME = "databaseUserName";
		
		/**
		 * The url for the database connection.
		 */
		public static final String DATABASE_URL = "databaseURL";
		
		/**
		 * The password of the user used to connect to the database.
		 */
		public static final String DATABASE_PASSWORD = "databasePassword";
		
		// Various Classes Variables Names
		
		/**
		 * Database Engine. (Example: PostgreSQL)
		 */
		public static final String DATABASE_ENGINE = "databaseEngine";
	}
	
	/**
	 * Contains constants with default values.
	 * 
	 * @author Sergio Morel
	 */
	public interface Defaults
	{
		//Integer values
		public static final Integer CONNECTION_TIMEOUT_SECONDS = 0;
		
		// Various Classes Instances
		public static final DatabaseEngine DATABASE_ENGINE = DatabaseEngine.POSTGRES;
	}
	
	/**
	 * Contains resources names
	 * 
	 * @author Sergio Morel
	 */
	public interface Resources
	{
		/**
		 * Base name of the boundle of properties files for each language.
		 */
		public static final String MESSAGES_BASE_NAME = "messages";
	}
	
	/**
	 * Contains the value of constants from the Utilities library.
	 * 
	 * @author Sergio Morel
	 */
	public static interface Utilities extends py.com.semp.lib.utilidades.configuration.Values {}
}