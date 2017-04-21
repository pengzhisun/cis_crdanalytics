package com.ehoo.DAO;

import com.ehoo.common.config.Config;
import com.ehoo.vo.Comment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.util.*;

/**
 * Created by guoqing.zhou on 2017/3/5.
 */
@Repository
public class SentimentDAO {
        private String driverName="com.microsoft.sqlserver.jdbc.SQLServerDriver";
        


        //查询电商未情感分析的数据
        public List<Map<String,Object>> getUnProcessData(int count){
                List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
                Connection con = null;
                Statement stmt = null;
                ResultSet rs = null;


                try {
                        Class.forName(driverName);
                        con= DriverManager.getConnection(Config.dataUrl);
                        String sql="SELECT TOP "+count+" c.id,c.processed,c.comment,c.rate_time,c.topic,c.sentimence,c.process_time,c.product_name,c.source from comment c" +
                                " WHERE c.processed = 0 ORDER BY c.create_time";
                        stmt = con.createStatement();
                        rs = stmt.executeQuery(sql);
                        // 循环结果集
                        ResultSetMetaData rsmd = rs.getMetaData();
                        int columnCount = rsmd.getColumnCount();
                        Map<String, Object> data = null;
                        while (rs.next()) {
                                data = new HashMap<String, Object>();
                                // 每循环一条将列名和列值存入Map
                                for (int i = 1; i <= columnCount; i++) {
                                        data.put(rsmd.getColumnLabel(i), rs.getObject(rsmd
                                                .getColumnLabel(i)));
                                }
                                // 将整条数据的Map存入到List中
                                datas.add(data);
                        }
                } catch (Exception e) {
                        e.printStackTrace();
                }finally {
                        if (rs != null) {
                                try {
                                        rs.close();
                                } catch(Exception e) {
                                        e.printStackTrace();
                                }
                        }
                        if (stmt != null) {
                                try {
                                        stmt.close();
                                } catch(Exception e) {
                                        e.printStackTrace();
                                }
                        }
                        if (con != null) {
                                try {
                                        con.close();
                                } catch(Exception e) {
                                        e.printStackTrace();
                                }
                        }
                }
                return datas;
        }

        public void updateCommentSentiment(Comment comment) {
                Connection con = null;
                PreparedStatement pComment = null;
                PreparedStatement pTopic = null;
                PreparedStatement pSentimence = null;
                PreparedStatement pHotKeys = null;
                try {
                        Class.forName(driverName);
                        con = DriverManager.getConnection(Config.dataUrl);
                        con.setAutoCommit(false);
                        String updateCommentSql = "UPDATE comment set processed = ?,topic = ?,sentimence = ?,process_time = ? WHERE id = ?";
                        pComment = con.prepareStatement(updateCommentSql);
                        pComment.setInt(1,1);
                        pComment.setString(2,comment.getTopic());
                        pComment.setString(3,comment.getSentimence());
                        pComment.setString(4,comment.getProcess_time());
                        pComment.setString(5,comment.getId());
                        pComment.execute();
                        String insertTopicSql = "INSERT INTO topic(id,comment_id,topic,source,rate_time,create_time) VALUES (?,?,?,?,?,?)";
                        pTopic = con.prepareStatement(insertTopicSql);
                        pTopic.setString(1,UUID.randomUUID().toString().replaceAll("-", ""));
                        pTopic.setString(2,comment.getId());
                        pTopic.setString(3,comment.getTopic());
                        pTopic.setString(4,comment.getSource());
                        pTopic.setString(5,comment.getRate_time());
                        pTopic.setString(6,comment.getProcess_time());
                        pTopic.execute();
                        String insertSentimenceSql = "INSERT INTO sentimence(id,comment_id,sentimence,source,rate_time,create_time) VALUES (?,?,?,?,?,?)";
                        pSentimence = con.prepareStatement(insertSentimenceSql);
                        pSentimence.setString(1,UUID.randomUUID().toString().replaceAll("-", ""));
                        pSentimence.setString(2,comment.getId());
                        pSentimence.setString(3,comment.getSentimence());
                        pSentimence.setString(4,comment.getSource());
                        pSentimence.setString(5,comment.getRate_time());
                        pSentimence.setString(6,comment.getProcess_time());
                        pSentimence.execute();
                        List<String> hotkeys = comment.hotkeys;
                        if (hotkeys.size() > 0){
                                String insertHotkeySql = "INSERT INTO hotkeys(id,comment_id,product,hotkey,source,rate_time,create_time) VALUES (?,?,?,?,?,?,?)";
                                pHotKeys = con.prepareStatement(insertHotkeySql);
                                for (int i = 0; i<hotkeys.size();i++){
                                        pHotKeys.setString(1,UUID.randomUUID().toString().replaceAll("-", ""));
                                        pHotKeys.setString(2,comment.getId());
                                        pHotKeys.setString(3,comment.getProduct_name());
                                        pHotKeys.setString(4,hotkeys.get(i));
                                        pHotKeys.setString(5,comment.getSource());
                                        pHotKeys.setString(6,comment.getRate_time());
                                        pHotKeys.setString(7,comment.getProcess_time());
                                        pHotKeys.addBatch();
                                }
                                pHotKeys.executeBatch();
                        }
                        con.commit();
                }catch (Exception e){
                        try {
                                con.rollback();
                        } catch (SQLException e1) {
                                e1.printStackTrace();
                        }
                        e.printStackTrace();

                }finally {
                        if (con != null) {
                                try {
                                        con.close();
                                } catch(Exception e) {
                                        e.printStackTrace();
                                }
                        }
                        if (pComment != null) {
                                try {
                                        pComment.close();
                                } catch(Exception e) {
                                        e.printStackTrace();
                                }
                        }
                        if (pTopic != null) {
                                try {
                                        pTopic.close();
                                } catch(Exception e) {
                                        e.printStackTrace();
                                }
                        }
                        if (pHotKeys != null) {
                                try {
                                        pHotKeys.close();
                                } catch(Exception e) {
                                        e.printStackTrace();
                                }
                        }
                }
        }
}
