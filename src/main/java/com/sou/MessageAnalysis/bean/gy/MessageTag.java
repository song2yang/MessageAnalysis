package com.sou.MessageAnalysis.bean.gy;

public class MessageTag {
    //标签主键
    private String id;
    // 短信UUID
    private String uuid;
    // 加密手机号
    private String telMd5;
    // 特服号和短信标识头对应
    private String serviceNo;
    // 短信标识头
    private String mark;
    // 标签码值
    private String tagKey;
    // 命中状况，金额类标签显示的是具体金额
    private String tagVal;
    // 发送年分
    private String year;
    // 发送月份
    private String month;
    // 发送日期
    private String dt;
    // 创建时间
    private String createTime;
    // 短信发送时间
    private String sendTime;
    // 短信标识，同一短信一致
    private String msgId;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getTelMd5() {
        return telMd5;
    }

    public void setTelMd5(String telMd5) {
        this.telMd5 = telMd5;
    }

    public String getServiceNo() {
        return serviceNo;
    }

    public void setServiceNo(String serviceNo) {
        this.serviceNo = serviceNo;
    }

    public String getMark() {
        return mark;
    }

    public void setMark(String mark) {
        this.mark = mark;
    }

    public String getTagKey() {
        return tagKey;
    }

    public void setTagKey(String tagKey) {
        this.tagKey = tagKey;
    }

    public String getTagVal() {
        return tagVal;
    }

    public void setTagVal(String tagVal) {
        this.tagVal = tagVal;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getSendTime() {
        return sendTime;
    }

    public void setSendTime(String sendTime) {
        this.sendTime = sendTime;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }
}
