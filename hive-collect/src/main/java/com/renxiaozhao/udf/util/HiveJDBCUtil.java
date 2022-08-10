package com.renxiaozhao.udf.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**.
 * JDBC连接工具类
 * @author https://renxiaozhao.blog.csdn.net/
 *
 */
public class HiveJDBCUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveJDBCUtil.class);
    
    public static Connection getConn() throws ClassNotFoundException {
        Connection conn = null;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager.getConnection("jdbc:hive2://192.168.38.10:10000/default", "root", "");// 连接数据库
        } catch (SQLException e) {
            LOGGER.error("HiveJDBCUtil.getConn()异常---->", e);
        }
        return conn;
    }

    public void closeConn(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
                conn = null;
            }
        } catch (SQLException e) {
            LOGGER.error("HiveJDBCUtil.closeConn()异常---->" + e);
        }
    }

    public static PreparedStatement getPStmt(Connection conn, String sql) {
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement(sql);
        } catch (SQLException e) {
            LOGGER.error("HiveJDBCUtil.getPStmt()异常---->" + e);
        }
        return pstmt;
    }

    public static void closePStmt(PreparedStatement stmt) {
        try {
            if (stmt != null) {
                stmt.close();
                stmt = null;
            }
        } catch (SQLException e) {
            LOGGER.error("HiveJDBCUtil.closePStmt()异常---->" + e);
        }

    }

    public static void closeRs(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
                rs = null;
            }
        } catch (SQLException e) {
            LOGGER.error("HiveJDBCUtil.closeRs()异常---->" + e);
        }
    }

    public ResultSet executeQuery(Connection conn, String sql) {
        ResultSet rs = null;
        try {
            rs = conn.createStatement().executeQuery(sql);
        } catch (SQLException e) {
            LOGGER.error("HiveJDBCUtil.executeQuery()异常---->", e);
        }
        return rs;
    }
    
}
