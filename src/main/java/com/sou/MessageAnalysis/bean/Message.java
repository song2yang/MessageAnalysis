package com.sou.MessageAnalysis.bean;
/**
 * Created by souyouyou on 2018/11/3.
 */
public class Message {
    private String tel;
    private String submitTime;
    private String content;
    private String appName;
    private Integer isHit;
    private String hitRuleCode;
    private String hitRuleValue;
    private String hitApp;

    public String getHitApp() {
        return hitApp;
    }

    public void setHitApp(String hitApp) {
        this.hitApp = hitApp;
    }

    public String getHitRuleCode() {
        return hitRuleCode;
    }

    public void setHitRuleCode(String hitRuleCode) {
        this.hitRuleCode = hitRuleCode;
    }

    public String getHitRuleValue() {
        return hitRuleValue;
    }

    public void setHitRuleValue(String hitRuleValue) {
        this.hitRuleValue = hitRuleValue;
    }

    public Integer getIsHit() {
        return isHit;
    }

    public void setIsHit(Integer isHit) {
        this.isHit = isHit;
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getSubmitTime() {
        return submitTime;
    }

    public void setSubmitTime(String submitTime) {
        this.submitTime = submitTime;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }
}