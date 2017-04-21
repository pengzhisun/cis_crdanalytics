package com.ehoo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ehoo.DAO.SentimentDAO;
import com.ehoo.common.config.Config;
import com.ehoo.common.util.DateUtils;
import com.ehoo.common.util.HttpRequestUtils;
import com.ehoo.vo.Comment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.URLEncoder;
import java.util.*;

/**
 * Created by guoqing.zhou on 2017/3/5.
 */
@Component
@EnableScheduling
public class SentimentProcessTask {
        @Autowired
        private SentimentDAO sentimentDAO;

        private static final int COUNT = 100;

        @Scheduled(fixedDelay = 5 * 1000)
        public void  sentimentProcess(){
                //从comment中取出未做情感分析的数据
                List<Map<String,Object>> comments = sentimentDAO.getUnProcessData(COUNT);
                System.out.println("本次获取待处理数据"+comments.size()+"条");
                List<String> success = new ArrayList<String>();
                List<String> fail = new ArrayList<String>();
                if (comments.size() > 0){
                        for (Map<String,Object> commentMap : comments){
                                String id = commentMap.get("id").toString();
                                try {
                                        String content = commentMap.get("comment").toString();
                                        Map<String,String> params = new HashMap<String,String>();
                                        params.put("query",content);
                                        String topicJson = HttpRequestUtils.doPost(Config.topic,params);
                                        JSONObject topic = JSONObject.parseObject(topicJson);
                                        String emotionJson = HttpRequestUtils.doPost(Config.emotion,params);
                                        JSONObject emotion = JSONObject.parseObject(emotionJson);
                                        String wordsJson = HttpRequestUtils.doPost(Config.words,params);
                                        JSONObject words = JSONObject.parseObject(wordsJson);
                                        /*System.out.println("topic:"+topicJson);
                                        System.out.println("emotionJson:"+emotionJson);
                                        System.out.println("wordsJson:"+wordsJson);*/
                                        Comment comment = new Comment();
                                        comment.setProduct_name(commentMap.get("product_name").toString());
                                        comment.setRate_time(commentMap.get("rate_time").toString());
                                        comment.setSource(commentMap.get("source").toString());
                                        comment.setId(id);
                                        comment.setProcessed(1);
                                        comment.setTopic(topic.getString("category"));
                                        comment.setSentimence(emotion.getString("score"));
                                        JSONArray hotkeys = words.getJSONArray("words");
                                        if(hotkeys != null && hotkeys.size() > 0){
                                                Iterator<Object> ih = hotkeys.iterator();
                                                while (ih.hasNext()){
                                                        comment.hotkeys.add(ih.next().toString());
                                                }
                                        }
                                        comment.setProcess_time(DateUtils.getCurrTime());
                                        sentimentDAO.updateCommentSentiment(comment);
                                        success.add(id);
                                }catch (Exception e){
                                        fail.add(id);
                                        e.printStackTrace();
                                }
                        }
                }
                System.out.println("本次任务成功："+ success.size() +";失败：" + fail.size());
        }
}
