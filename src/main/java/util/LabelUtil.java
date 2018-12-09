package util;

import com.sou.MessageAnalysis.bean.Message;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LabelUtil {
    public static JavaRDD<Object> markMsgApp(JavaRDD<String> lines, List appList){
        JavaRDD<Object> messages = lines.map(new Function<String, Object>() {

            public Message call(String msg) throws Exception {
                Message message = new Message();

                String[] msgInfo = msg.split("\t");
                String tel = null, submitTime = null, content = null, appName = null,hitAppName = null;
                if (msgInfo.length > 3) {
                    tel = msgInfo[0];
                    submitTime = msgInfo[1];
                    content = msg.substring(tel.length() + submitTime.length(), msg.length());
                } else if (msgInfo.length == 3) {
                    //掌众短信
                    tel = msgInfo[0];
                    submitTime = msgInfo[1];
                    content = msgInfo[2];
                } else {
                    tel = msgInfo[0];
                    submitTime = msgInfo[1];
                    content = "";
                }

                Pattern pattern = Pattern.compile("(?<=【)(.+?)(?=】)");
                Matcher matcher = pattern.matcher(content);

                if (matcher.find()) {
                    appName = matcher.group();

                    for (int i = 0; i < appList.size(); i++) {
                        String appNameStr = String.valueOf(appList.get(i));
                        appNameStr = appNameStr.substring(1, appNameStr.length() - 1);
                        if (appName.contains(String.valueOf(appNameStr)) || content.contains(String.valueOf(appNameStr))) {
                            hitAppName = String.valueOf(appList.get(i));
                            break;
                        } else {
                            hitAppName = "N";
                        }
                    }


                } else {
                    for (int i = 0; i < appList.size(); i++) {
                        String appNameStr = String.valueOf(appList.get(i));
                        appNameStr = appNameStr.substring(1, appNameStr.length() - 1);
                        if (content.contains(String.valueOf(appNameStr))) {
                            hitAppName = String.valueOf(appList.get(i));
                            break;
                        } else {
                            hitAppName = "N";
                        }
                    }

                    appName = "N";
                }

                message.setTel(tel);
                message.setAppName(appName);
                message.setContent(content);
                message.setSubmitTime(submitTime);
                message.setHitApp(hitAppName);

                return message;
            }

        });

        return messages;
    }


    public static JavaRDD<Object> markMsgrule(JavaRDD<Row> hitAppMsgRdd, List ruleList){
        JavaRDD<Object> hitMsgRuleRdd = hitAppMsgRdd.map(new Function<Row, Object>() {
            @Override
            public Message call(Row row) throws Exception {
                Message message = new Message();
                String msgStr = "";

                String appName = row.getString(0);
                appName = appName.substring(1, appName.length() - 1);
                String content = row.getString(1);
                String submitTime = row.getString(5);
                String tel = row.getString(6);
                Integer isHit = 0;
                String hitRule = "N";
                String hitRuleCode = "";
                String hitRuleValue = "";
                for (Object object : ruleList) {
                    String[] rules = String.valueOf(object).substring(1, String.valueOf(object).length() - 1).split(",");
                    String ruleApp = rules[0];
                    String ruleCode = rules[1];
                    String ruleValue = rules[2];
                    String[] rule = ruleValue.split("&");
                    if (appName.equals(ruleApp)) {
                        boolean hitFlag = true;
                        for (String ruleVal : rule) {
                            if (!content.contains(ruleVal)) {
                                hitFlag = false;
                                break;
                            }
                        }

                        if (hitFlag) {
                            isHit = 1;
                            hitRule = ruleValue;
                            hitRuleCode = ruleCode;
                            hitRuleValue = ruleValue;
//                            System.out.println(content+">>>>"+ruleApp+">>>>"+ruleValue);
                            break;
                        }

                    } else {
                        continue;
                    }
                }
                msgStr = appName + "," + content + "," + submitTime + "," + tel + "," + isHit;
//                System.out.println(msgStr + "," + hitRule);
                message.setAppName(appName);
                message.setContent(content);
                message.setSubmitTime(submitTime);
                message.setTel(tel);
                message.setIsHit(isHit);
                message.setHitRuleCode(hitRuleCode);
                message.setHitRuleValue(hitRuleValue);
                return message;
            }
        });


        return hitMsgRuleRdd;
    }

}
