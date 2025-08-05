package py.com.semp.lib.database.connection;

/**
 * Enum representing supported database engines and their associated JDBC driver class names.
 * <p>
 * This abstraction provides type-safe selection of known database engines and exposes
 * the fully qualified class name of their corresponding JDBC driver for use with {@code Class.forName(...)}.
 * </p>
 *
 * You can retrieve the driver class for registration or diagnostics using {@link #getDriverClass()}.
 */
public enum DatabaseEngine
{
    /** MySQL or MariaDB via Connector/J driver. */
    MYSQL("com.mysql.cj.jdbc.Driver"),

    /** PostgreSQL via the official PostgreSQL JDBC driver. */
    POSTGRES("org.postgresql.Driver"),

    /** SQLite using the Xerial SQLite JDBC driver. */
    SQLITE("org.sqlite.JDBC"),

    /** Microsoft SQL Server using the Microsoft JDBC driver. */
    SQLSERVER("com.microsoft.sqlserver.jdbc.SQLServerDriver"),

    /** IBM Db2 via the IBM Data Server Driver for JDBC and SQLJ. */
    DB2("com.ibm.db2.jcc.DB2Driver"),

    /** Oracle Database using the Oracle JDBC Thin driver. */
    ORACLE("oracle.jdbc.OracleDriver"),

    /** H2 Database Engine (in-memory or file-based). */
    H2("org.h2.Driver"),

    /** HyperSQL Database (HSQLDB). */
    HSQLDB("org.hsqldb.jdbc.JDBCDriver"),

    /** Firebird SQL via Jaybird JDBC driver. */
    FIREBIRD("org.firebirdsql.jdbc.FBDriver"),

    /** SAP Sybase Adaptive Server Enterprise (ASE) using jConnect. */
    SYBASE("com.sybase.jdbc4.jdbc.SybDriver");

    private final String driverClass;

    DatabaseEngine(String driverClass)
    {
        this.driverClass = driverClass;
    }

    /**
     * Returns the fully qualified JDBC driver class name for this database engine.
     *
     * @return the JDBC driver class name.
     */
    public String getDriverClass()
    {
        return driverClass;
    }
}