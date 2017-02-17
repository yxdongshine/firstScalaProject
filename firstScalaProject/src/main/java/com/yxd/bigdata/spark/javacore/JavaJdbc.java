package com.yxd.bigdata.spark.javacore;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by 20160905 on 2017/2/17.
 */
public class JavaJdbc {

    private  static String url = "jdbc:mysql://hadoop1:3306/metastore";
    private  static String username = "root";
    private  static String password = "root";
    private  static Connection conn = null;

    private  static  JavaJdbc javaJdbcInstance = null ;
    private  JavaJdbc(){

    }

    public  static  JavaJdbc getInstance(){
        if(javaJdbcInstance == null){
            javaJdbcInstance = new JavaJdbc();
        }
        return  javaJdbcInstance ;
    }


    public  Connection  getConn(){

        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        try {
            conn = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return  conn ;
    }



    public    void  insert(DataEntry dataEntry) throws SQLException {
        Connection conn = getConn();
        // 2. 构建statement
        String sql = "insert into dataEntry values(?,?)";

        try {
            PreparedStatement pstmt = conn.prepareStatement(sql);

            // 3. 结果数据输出
                pstmt.setString(1, dataEntry.getKey());
                pstmt.setLong(2, dataEntry.getValue());

                pstmt.executeUpdate();

        }catch (Exception e){
            e.getMessage();
        }
    }


    /**
     *  最后关闭
     */
    public   void closeConn(){
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
