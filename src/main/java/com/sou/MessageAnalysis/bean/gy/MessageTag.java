package com.sou.MessageAnalysis.bean.gy;

public class MessageTag {
    //标签主键
    private String id;
    // 短信UUID
//    private String uuid;
    // 加密手机号
    private String telMd5;
    // 特服号和短信标识头对应
//    private String serviceNo;
    // 短信标识头
//    private String mark;
    // 标签码值
    private String tagKey;

    public Double getTagVal() {
        return tagVal;
    }

    public void setTagVal(Double tagVal) {
        this.tagVal = tagVal;
    }

    // 命中状况，金额类标签显示的是具体金额
    private Double tagVal;
    // 发送年分
//    private String year;
    // 发送月份
//    private String month;
    // 发送日期
//    private String dt;
    // 创建时间
//    private String createTime;
    // 短信发送时间
    private String sendTime;
    // 短信标识，同一短信一致
//    private String msgId;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTelMd5() {
        return telMd5;
    }

    public void setTelMd5(String telMd5) {
        this.telMd5 = telMd5;
    }

    public String getTagKey() {
        return tagKey;
    }

    public void setTagKey(String tagKey) {
        this.tagKey = tagKey;
    }

    public String getSendTime() {
        return sendTime;
    }

    public void setSendTime(String sendTime) {
        this.sendTime = sendTime;
    }
}
