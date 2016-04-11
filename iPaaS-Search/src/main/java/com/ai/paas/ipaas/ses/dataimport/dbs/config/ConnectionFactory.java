package com.ai.paas.ipaas.ses.dataimport.dbs.config;

import com.mysql.jdbc.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Connection Factory
 * <p/>
 * Created by gaoht on 15/6/4.
 */
public class ConnectionFactory {

    private static final Logger log = LoggerFactory.getLogger(ConnectionFactory.class);
    private ConnectionFactory(){
    	
    }

    public static Connection getConn(String connStr) throws SQLException {
        if (connStr.startsWith("dt@mysql:"))
			connStr = connStr.substring(9, connStr.length());
        Properties p = new Properties();
        p.setProperty("max_allowed_packet", "20971520");

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(connStr, p);
            log.debug("{} Connected.", connStr);
        } catch (SQLException ex) {
            // handle any errors
            log.error("SQLException: " + ex.getMessage(),ex);
            throw ex;
        }
        return conn;
    }

    public static boolean test(Connection conn) {
        try {
            return conn.createStatement().execute("select 1");
        } catch (Exception e) {
            log.error("test error", e);
            return false;
        }
    }

    public static Statement createStatement(Connection conn) throws SQLException {
        return (com.mysql.jdbc.Statement) conn.createStatement();
    }
}
