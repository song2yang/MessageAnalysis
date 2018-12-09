package com.sou.MessageAnalysis.company;

import com.sou.MessageAnalysis.App;
import com.sou.MessageAnalysis.bean.gy.Message;
import com.sou.MessageAnalysis.bean.gy.MessageTag;
import com.sou.MessageAnalysis.bean.gy.SampleInfo;
import com.sou.MessageAnalysis.bean.gy.TelPhone;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import util.HdfsUtil;

import java.text.SimpleDateFormat;

public class GyFintech {
    private static Logger logger = Logger.getLogger(GyFintech.class);

    public static void msgStatistics(JavaSparkContext jsc, SQLContext sc, String hdfsHost, String sourcePath){
        /**
         * 1、短信号码（样本）在短信中的覆盖率
         * 2、样本中号码的短信数量分布
         * 3、样本中（申请时间）往前推最晚（近）短信的时间减去申请时间（天数）分布
         * 4、样本中（申请时间）往前推最早（远）短信的时间减去申请时间（天数）分布
         */

        Dataset<Row> telDs = getTelRdd(jsc, sc, hdfsHost, sourcePath);
        telDs.registerTempTable("telPhone");
        //样本用户总数量
        Long sampleCount = telDs.count();

        Dataset<Row> msgDs = getMsgRdd(jsc, sc, hdfsHost, sourcePath);
        msgDs.registerTempTable("msg");

        Dataset<Row> sampleDs = getSampleRdd(jsc, sc, hdfsHost, sourcePath);
//        sampleDs.show();

        Dataset<Row> sampleTelDs = sampleDs.join(telDs, sampleDs.col("mobile").equalTo(telDs.col("originalNo"))).distinct();
//        sampleTelDs.show();
//        sc.sql("select applicationDt,to_date(applicationDt) from sampleInfo").show();

        Dataset<Row> sampleTelMsgDs = sampleTelDs.join(msgDs, sampleTelDs.col("md5No").equalTo(msgDs.col("tel")), "left");
        sampleTelMsgDs.registerTempTable("sampleInfo");

        sampleTelMsgDs.show();

        Dataset<Row> msgSampleDs = sc.sql("select count(distinct md5No) from sampleInfo where content != ''");
        //短信中样本覆盖数量
        Long sampleInMsgCount = msgSampleDs.count();
        //短信号码（样本）在短信中的覆盖率
        logger.error("短信号码（样本）在短信中的覆盖率:"+sampleInMsgCount+"/"+sampleCount+"="+sampleInMsgCount/sampleCount);

        HdfsUtil.deleteFile("/result/gy/UserMsgCount");
//        msgSampleDs.show();
        //样本中号码的短信数量分布
        sc.sql("select md5No,count(*) from sampleInfo where content != '' GROUP BY md5No")
                .write().csv("hdfs://10.0.1.95:9000/result/gy/UserMsgCount");
        //样本中（申请时间）往前推最晚（近）短信的时间减去申请时间（天数）分布
        HdfsUtil.deleteFile("/result/gy/lastDt");
        sc.sql("select md5No,datediff(to_date(max(submitTime)),to_date(first(applicationDt))) from sampleInfo where content != '' group by md5No")
                .write().csv("hdfs://10.0.1.95:9000/result/gy/lastDt");

        //样本中（申请时间）往前推最早（远）短信的时间减去申请时间（天数）分布
        HdfsUtil.deleteFile("/result/gy/nearestDt");
        sc.sql("select md5No,datediff(to_date(first(applicationDt)),to_date(min(submitTime))) from sampleInfo where content != '' group by md5No")
                .write().csv("hdfs://10.0.1.95:9000/result/gy/nearestDt");

//        Properties prop = new Properties();
//        prop.setProperty("user","root");
//        prop.setProperty("password","root");
//
//
//
//        sc.sql("select * from msgInfo").write().jdbc("jdbc:mysql://10.0.1.95:3306/ym","msg_info",prop);


//        totalDs.show();





    }
    protected static  Dataset<Row>  getTelRdd(JavaSparkContext jsc, SQLContext sc,String hdfsHost, String sourcePath){
        JavaRDD<String> lines = jsc.textFile(hdfsHost+sourcePath +"CJM_1129_DE.txt");

        JavaRDD<Object> telRdd = lines.map(new Function<String, Object>() {
            @Override
            public Object call(String line) throws Exception {
                TelPhone telPhone = new TelPhone();

                String[] telArr = line.split("\t");
                telPhone.setOriginalNo(telArr[0]);
                telPhone.setMd5No(telArr[1]);
                return telPhone;
            }
        });

        Dataset<Row> telDf = sc.createDataFrame(telRdd, TelPhone.class);
//        telDf.registerTempTable("telPhone");
        return telDf;
    }

    protected static Dataset<Row> getMsgRdd(JavaSparkContext jsc, SQLContext sc,String hdfsHost, String sourcePath){
        JavaRDD<String> msgLines = jsc.textFile(hdfsHost+sourcePath +"test.csv");

        //读取原始短信
        JavaRDD<Object> msgRdd = msgLines.map(new Function<String, Object>() {
            @Override
            public Message call(String msg) throws Exception {
                Message message = new Message();

                String[] msgInfo = msg.split(",");
                String tel, submitTime, content;
                if (msgInfo.length > 3) {
                    tel = msgInfo[1];
                    submitTime = msgInfo[2];
                    content = msgInfo[3];
                } else if (msgInfo.length == 3) {
                    tel = msgInfo[1];
                    submitTime = msgInfo[2];
                    content = msgInfo[3];
                } else {
                    tel = msgInfo[1];
                    submitTime = msgInfo[2];
                    content = "";
                }

                message.setTel(tel.substring(1,tel.length()-1));
                if (content.length()>2){
                    message.setContent(content.substring(1,content.length()-1));
                }else {
                    System.out.println(content);
                    message.setContent(content);
                }

                message.setSubmitTime(submitTime.substring(1,submitTime.length()-1));

                return message;
            }
        });

        Dataset<Row> msgDf = sc.createDataFrame(msgRdd, Message.class);
//        msgDf.registerTempTable("message");

        return msgDf;
    }

    protected static void getMsgTagRdd(JavaSparkContext jsc,SQLContext sc,String hdfsHost,String sourcePath){
        JavaRDD<String> msgTagLines = jsc.textFile(hdfsHost+sourcePath +"zz_test.csv");

        JavaRDD<Object> msgTagRdd = msgTagLines.map(new Function<String, Object>() {
            @Override
            public Object call(String msgTag) throws Exception {

                MessageTag messageTag = new MessageTag();
                String[] msgTagInfo = msgTag.split(",");

                messageTag.setId(msgTagInfo[0].replaceAll("\"",""));
                messageTag.setUuid(msgTagInfo[1].replaceAll("\"",""));
                messageTag.setTelMd5(msgTagInfo[2].replaceAll("\"",""));
                messageTag.setServiceNo(msgTagInfo[3].replaceAll("\"",""));
                messageTag.setMark(msgTagInfo[4].replaceAll("\"",""));
                messageTag.setTagKey(msgTagInfo[5].replaceAll("\"",""));
                messageTag.setTagVal(msgTagInfo[6].replaceAll("\"",""));
                messageTag.setYear(msgTagInfo[7].replaceAll("\"",""));
                messageTag.setMonth(msgTagInfo[8].replaceAll("\"",""));
                messageTag.setDt(msgTagInfo[9].replaceAll("\"",""));
                messageTag.setCreateTime(msgTagInfo[10].replaceAll("\"",""));
                messageTag.setSendTime(msgTagInfo[11].replaceAll("\"",""));
                messageTag.setMsgId(msgTagInfo[12].replaceAll("\"",""));

                return messageTag;
            }
        });

        Dataset<Row> msgTagDf = sc.createDataFrame(msgTagRdd, MessageTag.class);
        msgTagDf.registerTempTable("msgTag");

        msgTagDf.show();
    }

    private static Dataset<Row> getSampleRdd(JavaSparkContext jsc,SQLContext sc,String hdfsHost,String sourcePath){
        JavaRDD<String> lines = jsc.textFile(hdfsHost+sourcePath +"YB_DE.csv");

        JavaRDD<Object> sampleRdd = lines.map(new Function<String, Object>() {
            @Override
            public Object call(String line) throws Exception {
                String[] sampleArr = line.split(",");

                SampleInfo sample = new SampleInfo();
                SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy/MM/dd");
                SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
                sample.setMobile(sampleArr[0]);
                sample.setApplicationDt(sdf2.format(sdf1.parse(sampleArr[1])));
                sample.setOverdueDays(sampleArr[2]);
                return sample;
            }
        });

        Dataset<Row> sampleDs = sc.createDataFrame(sampleRdd, SampleInfo.class);
        sampleDs.registerTempTable("sampleInfo");


        return sampleDs;

    }
}
