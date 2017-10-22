package com.zyuc.iot.stat.hbase;

import com.zyuc.stat.properties.ConfigProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.*;

/**
 * Created by zhoucw on 17-10-20.
 */
public class HbaseData2Oracle {
    static Configuration conf = null;
    static String driverUrl = "";
    static String dbUser = "";
    static String dbPasswd = "";

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT());
        conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM());
        driverUrl = "jdbc:oracle:thin:@100.66.124.129:1521/dbnms";
        dbUser  = "epcslview";
        dbPasswd = "epc_slview129";
    }


    public static Connection getConnection(){
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void getResultScann(String tableName, String start_rowkey,
                                      String stop_rowkey) throws IOException, SQLException, ClassNotFoundException {

        String monthid = "201705";
        String deleteSQL = "delete from  iot_stat_company_usernum where monthid='" + monthid + "' ";
        java.sql.Connection dbConn = DriverManager.getConnection(driverUrl, dbUser, dbPasswd);
        PreparedStatement psmt = null;
        psmt = dbConn.prepareStatement(deleteSQL);
        psmt.executeUpdate();
        psmt.close();

        Class.forName("oracle.jdbc.driver.OracleDriver");

        Connection conn = getConnection();

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        TableName tName = TableName.valueOf(tableName);
        Table table = conn.getTable(tName);
        String sql = "INSERT INTO iot_stat_company_Usernum (monthid,companycode) VALUES (?,?)";
        ResultScanner rs = null;
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                String row = new String(r.getRow());
                String code = new String(r.getValue(Bytes.toBytes("u"), Bytes.toBytes("ccode")));
                String tsum = new String(r.getValue(Bytes.toBytes("u"), Bytes.toBytes("tsum")));
                String dsum = new String(r.getValue(Bytes.toBytes("u"), Bytes.toBytes("dsum")));
                String vsum = new String(r.getValue(Bytes.toBytes("u"), Bytes.toBytes("vsum")));
                String drank = new String(r.getValue(Bytes.toBytes("u"), Bytes.toBytes("drank")));
                String vrank =new String(r.getValue(Bytes.toBytes("u"), Bytes.toBytes("vrank")));
                System.out.println("row:" + row + " code:" + code + " tsum:" + tsum + " dsum:" + dsum + " vsum:" + vsum + " drank:" + drank + " vrank:" + vrank);
            }
        } finally {
            rs.close();
            conn.close();
        }
    }

    public static void main(String[] args) {
        try {
            getResultScann("iot_stat_company_usernum", "201710_C_0", "201710_C_100");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
