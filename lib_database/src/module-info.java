/**
 * @author Sergio Morel
 */
module lib_database
{
	exports py.com.semp.lib.database.configuration;
	exports py.com.semp.lib.database.connection;
	exports py.com.semp.lib.database.utilities;
	
	requires transitive java.sql;
	requires transitive lib_utilidades;
}