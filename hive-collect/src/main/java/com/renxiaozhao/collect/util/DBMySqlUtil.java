package com.renxiaozhao.collect.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**.
 * JDBC连接工具类
 * @author admin
 *
 */
public class DBMySqlUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(DBMySqlUtil.class);
    
//    public static Connection getConn(String url, String user, String password) throws ClassNotFoundException {
    public static Connection getConn() throws ClassNotFoundException {
        Connection conn = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://192.168.38.10:3306/dolphinscheduler?characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&rewriteBatchedStatements=true",
                    "ds_user","dolphinscheduler");// 连接数据库
        } catch (SQLException e) {
            LOGGER.error("DBMySqlConfig.getConn()异常---->", e);
        }
        return conn;
    }

    public static boolean testColletHiveMeta(String value) throws SQLException, ClassNotFoundException {
        Connection con = getConn();
        String sql = "insert into test_hive(meta_value) values('"+value+"')";
        PreparedStatement pst = con.prepareStatement(sql);
        return pst.execute(sql);
    }
    public void closeConn(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
                conn = null;
            }
        } catch (SQLException e) {
            LOGGER.error("DBMySqlConfig.closeConn()异常---->", e);
        }
    }

    public PreparedStatement getPStmt(Connection conn, String sql) {
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement(sql);
        } catch (SQLException e) {
            LOGGER.error("DBMySqlConfig.getPStmt()异常---->", e);
        }
        return pstmt;
    }

    public void closePStmt(PreparedStatement stmt) {
        try {
            if (stmt != null) {
                stmt.close();
                stmt = null;
            }
        } catch (SQLException e) {
            LOGGER.error("DBMySqlConfig.closePStmt()异常---->", e);
        }

    }

    public void closeRs(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
                rs = null;
            }
        } catch (SQLException e) {
            LOGGER.error("DBMySqlConfig.closeRs()异常---->", e);
        }
    }

    public ResultSet executeQuery(Connection conn, String sql) {
        ResultSet rs = null;
        try {
            rs = conn.createStatement().executeQuery(sql);
        } catch (SQLException e) {
            LOGGER.error("DBMySqlConfig.executeQuery()异常---->", e);
        }
        return rs;
    }
}