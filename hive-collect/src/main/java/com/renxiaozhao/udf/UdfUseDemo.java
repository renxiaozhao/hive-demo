package com.renxiaozhao.udf;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.renxiaozhao.udf.util.HiveJDBCUtil;

/**
 * UdfUseDemo.
 * @author https://renxiaozhao.blog.csdn.net/
 */
public class UdfUseDemo {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Connection conn = HiveJDBCUtil.getConn();
        //掩码函数rxz_mask
        String maskSql1 = "select rxz_mask(phone,0,4) as maskPhone from  rxz_udf_test";
        String maskSql2 = "select rxz_mask(phone,-1,4) as maskPhone from  rxz_udf_test";
        String maskSql3 = "select rxz_mask(phone,3,4) as maskPhone from  rxz_udf_test";
        PreparedStatement maskPstmt1 = HiveJDBCUtil.getPStmt(conn,maskSql1);
        PreparedStatement maskPstmt2 = HiveJDBCUtil.getPStmt(conn,maskSql2);
        PreparedStatement maskPstmt3 = HiveJDBCUtil.getPStmt(conn,maskSql3);
        ResultSet maskRes1 = maskPstmt1.executeQuery();
        ResultSet maskRes2 = maskPstmt2.executeQuery();
        ResultSet maskRes3 = maskPstmt3.executeQuery();
        while (maskRes1.next()) {
            System.out.println("掩码 rxz_mask(phone,0,4)：" + maskRes1.getString("maskPhone"));
        }
        while (maskRes2.next()) {
            System.out.println("掩码 rxz_mask(phone,-1,4)：" + maskRes2.getString("maskPhone"));
        }
        while (maskRes3.next()) {
            System.out.println("掩码 rxz_mask(phone,3,4)：" + maskRes3.getString("maskPhone"));
        }
        HiveJDBCUtil.closePStmt(maskPstmt1);
        HiveJDBCUtil.closePStmt(maskPstmt2);
        HiveJDBCUtil.closePStmt(maskPstmt3);
        HiveJDBCUtil.closeRs(maskRes1);
        HiveJDBCUtil.closeRs(maskRes2);
        HiveJDBCUtil.closeRs(maskRes3);
        //截断函数rxz_cutoff
        conn = HiveJDBCUtil.getConn();
        String cutoffSql1 = "select rxz_cutoff(phone,0,4) as cutoffPhone from  rxz_udf_test";
        String cutoffSql2 = "select rxz_cutoff(phone,-1,4) as cutoffPhone from  rxz_udf_test";
        String cutoffSql3 = "select rxz_cutoff(phone,3,4) as cutoffPhone from  rxz_udf_test";
        PreparedStatement cutoffPstmt1 = HiveJDBCUtil.getPStmt(conn,cutoffSql1);
        PreparedStatement cutoffPstmt2 = HiveJDBCUtil.getPStmt(conn,cutoffSql2);
        PreparedStatement cutoffPstmt3 = HiveJDBCUtil.getPStmt(conn,cutoffSql3);
        ResultSet cutoffRes1 = cutoffPstmt1.executeQuery();
        ResultSet cutoffRes2 = cutoffPstmt2.executeQuery();
        ResultSet cutoffRes3 = cutoffPstmt3.executeQuery();
        while (cutoffRes1.next()) {
            System.out.println("截断 rxz_cutoff(phone,0,4)：" + cutoffRes1.getString("cutoffPhone"));
        }
        while (cutoffRes2.next()) {
            System.out.println("截断 rxz_cutoff(phone,-1,4)：" + cutoffRes2.getString("cutoffPhone"));
        }
        while (cutoffRes3.next()) {
            System.out.println("截断 rxz_cutoff(phone,3,4)：" + cutoffRes3.getString("cutoffPhone"));
        }
        HiveJDBCUtil.closePStmt(cutoffPstmt1);
        HiveJDBCUtil.closePStmt(cutoffPstmt2);
        HiveJDBCUtil.closePStmt(cutoffPstmt3);
        HiveJDBCUtil.closeRs(cutoffRes1);
        HiveJDBCUtil.closeRs(cutoffRes2);
        HiveJDBCUtil.closeRs(cutoffRes3);
    }
}
