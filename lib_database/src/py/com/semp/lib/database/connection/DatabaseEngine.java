package py.com.semp.lib.database.connection;

public enum DatabaseEngine
{
	MYSQL("com.mysql.cj.jdbc.Driver"),
	POSTGRES("org.postgresql.Driver"),
	SQLITE("org.sqlite.JDBC"),
	SQLSERVER("com.microsoft.sqlserver.jdbc.SQLServerDriver"),
	DB2("com.ibm.db2.jcc.DB2Driver");
	
	private final String driverClass;
	
	DatabaseEngine(String driverClass)
	{
		this.driverClass = driverClass;
	}
	
	public String getDriverClass()
	{
		return driverClass;
	}
}