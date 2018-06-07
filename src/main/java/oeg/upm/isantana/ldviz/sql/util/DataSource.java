package oeg.upm.isantana.ldviz.sql.util;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;

//https://www.javatips.net/blog/dbcp-connection-pooling-example
public class DataSource {

    private static DataSource  datasource;
    private BasicDataSource ds;
    

    //TODO read from server config or property file
    // JDBC driver name and database URL
    /*
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
    static final String DB_URL = "jdbc:mysql://localhost:8889/graphless";

    //  Database credentials
    static final String USER = "root";
    static final String PASS = "root";
    */

    //BARAJAS
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
    static final String DB_URL = "jdbc:mysql://localhost:3306/graphless";

    //  Database credentials
    static final String USER = "isantana";
    static final String PASS = "isp2018";
    

    private DataSource() throws IOException, SQLException, PropertyVetoException {
        ds = new BasicDataSource();
        ds.setDriverClassName(JDBC_DRIVER);
        ds.setUsername(USER);
        ds.setPassword(PASS);
        ds.setUrl(DB_URL);
       
     // the settings below are optional -- dbcp can work with defaults
        ds.setMinIdle(5);
        ds.setMaxIdle(20);
        ds.setMaxOpenPreparedStatements(180);

    }

    public static DataSource getInstance() throws IOException, SQLException, PropertyVetoException {
        if (datasource == null) {
            datasource = new DataSource();
            return datasource;
        } else {
            return datasource;
        }
    }

    public Connection getConnection() throws SQLException {
        return this.ds.getConnection();
    }

}
