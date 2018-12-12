package com.sou.MessageAnalysis.company;

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

    private static final String fileType = "DE";

    public static void derivedVars(JavaSparkContext jsc, SQLContext sc, String hdfsHost, String sourcePath,Logger logger){
        /**
         * 根据规则衍生变量
         */

        //样本原始文件 大额 CJM_1129_DE 小额 CJM_1129_XE
        Dataset<Row> telDs = getTelRdd(jsc, sc, hdfsHost, sourcePath,"CJM_1129_"+fileType+".txt").distinct();
        telDs.registerTempTable("telPhone");

        //样本手机号码md5文件 大额 YB_DE 小额 YB_XE
        Dataset<Row> sampleDs = getSampleRdd(jsc, sc, hdfsHost, sourcePath, "YB_"+fileType+".csv").distinct();

        Dataset<Row> sampleTelDs = sampleDs.join(telDs, sampleDs.col("mobile").equalTo(telDs.col("originalNo"))).distinct();
        sampleTelDs.show();

        //样本对应短信 大额 DE 小额 XE
        Dataset<Row> msgTagDs = getMsgTagRdd(jsc, sc, hdfsHost, sourcePath,"zz_tag_"+fileType+"_20w.csv").distinct();
        msgTagDs.registerTempTable("msg");

        Dataset<Row> sampleTagDs = sampleTelDs.join(msgTagDs, msgTagDs.col("telMd5").equalTo(sampleTelDs.col("md5No")));
        sampleTagDs.registerTempTable("sampleTagTemp");
        write2csv(sampleTagDs,"sampleTagDs");


        sampleTagDs = sc.sql("select createTime,dt,id,mark,month,msgId,sendTime,serviceNo,tagKey,tagVal,uuid,year,applicationDt," +
                "mobile,overdueDays,md5No from sampleTagTemp");
        sampleTagDs.registerTempTable("sampleTag");

        /**
         * 基础变量
         * 统计指标	说明
         * CNT	总笔数
         * AMT	总金额
         * MAX	最大金额
         * MIN	最小金额
         * AVG	平均金额
         * VAR	方差金额
         * KURT	峰度
         * SKEW	偏度
         * MED	中位数
         * 25Q	25分位
         * 75Q	75分位
         * AVGS	新平均金额（除以对应天数）
         * SKEWS	新偏度（使用新的平均值）
         * KURTS	新峰度（使用新的平均值）
         * DAYS	有流水的天数
         * 5000DAYS	金额大于5000元的天数
         * 1000DAYS	金额大于1000元的天数
         * 3000DAYS	金额大于3000元的天数
         * 1NUM	金额小于1元的数量
         * 1AMT	金额小于1元的金额
         * 2AMT	金额大于10000元的数量
         * 3AMT	金额大于10000元的金额
         * 4AMT	金额小于1元的数量占比
         * 5AMT	金额小于1元的金额占比
         * 6AMT	金额大于10000元的数量占比
         * 7AMT	金额大于10000元的金额占比
         * 8AMT	金额大于500元的天数
         */

        /* CNT 总笔数 */

        Dataset<Row> cntDs = sc.sql("select md5No ,count(*) from sampleTag group by md5No");
        cntDs.show();



        /* AMT	总金额 */
        /* MAX	最大金额 */
        /* MIN	最小金额 */
        /* AVG	平均金额 */
        /* VAR	方差金额 */
        /* KURT	峰度 */
        /* SKEW	偏度 */

    }

    public static void msgStatistics(JavaSparkContext jsc, SQLContext sc, String hdfsHost, String sourcePath,Logger logger){
        /**
         * 1、短信号码（样本）在短信中的覆盖率
         * 2、样本中号码的短信数量分布
         * 3、样本中（申请时间）往前推最晚（近）短信的时间减去申请时间（天数）分布
         * 4、样本中（申请时间）往前推最早（远）短信的时间减去申请时间（天数）分布
         */
        //样本原始文件 大额 CJM_1129_DE 小额 CJM_1129_XE
        Dataset<Row> telDs = getTelRdd(jsc, sc, hdfsHost, sourcePath,"CJM_1129_"+fileType+".txt").distinct();
        telDs.registerTempTable("telPhone");

        //样本用户总数量
        Long sampleCount = telDs.count();
        //样本对应短信 大额 DE 小额 XE
        Dataset<Row> msgDs = getMsgRdd(jsc, sc, hdfsHost, sourcePath,fileType+".csv");
        msgDs.registerTempTable("msg");

        //样本手机号码md5文件 大额 YB_DE 小额 YB_XE
        Dataset<Row> sampleDs = getSampleRdd(jsc, sc, hdfsHost, sourcePath, "YB_"+fileType+".csv").distinct();

        Dataset<Row> sampleTelDs = sampleDs.join(telDs, sampleDs.col("mobile").equalTo(telDs.col("originalNo"))).distinct();
        write2csv(sampleTelDs,"样本+加密号码");

        Dataset<Row> sampleTelMsgDs = msgDs.join(sampleTelDs, sampleTelDs.col("md5No").equalTo(msgDs.col("tel")), "left_outer");
        write2csv(sampleTelMsgDs,"样本+加密号码+短信文件");
        sampleTelMsgDs.registerTempTable("totalSampleInfo");

        //样本中号码的短信数量分布
        Dataset<Row> allUserMsgCountDs = sc.sql("select distinct(md5No),count(*) from totalSampleInfo where content != '' GROUP BY md5No ");
        write2csv(allUserMsgCountDs,"ALL用户短信数量");

        //未匹配的手机号码
        Dataset<Row> allUnmatchDs = sc.sql("select distinct(md5No) from totalSampleInfo where content is null");
        write2csv(allUnmatchDs,"ALL未匹配短信用户");

        Dataset<Row> sample2YearDs = sc.sql("select applicationDt,originalNo,overdueDays,md5No,content,submitTime,sid from totalSampleInfo where content != '' and abs(datediff(to_date(submitTime),to_date(applicationDt))) <= 700");
        sample2YearDs.registerTempTable("sampleInfo");
        write2csv(sample2YearDs,"2YEAR匹配用户短信");

        //样本中号码的短信数量分布
        Dataset<Row> twoYearUserMsgCountDs = sc.sql("select distinct(md5No),count(*) from sampleInfo where content != '' GROUP BY md5No ");
        write2csv(twoYearUserMsgCountDs,"2YEAR用户短信数量");

        Dataset<Row> msgSampleDs = sc.sql("select distinct(md5No) from sampleInfo where content != ''").distinct();
        write2csv(msgSampleDs,"2YEAR匹配短信用户");

        //未匹配的手机号码
        Dataset<Row> unmatchDs = sc.sql("select distinct(md5No) from sampleInfo where content is null");
        write2csv(unmatchDs,"2YEAR未匹配短信用户");

        //短信中样本覆盖数量
        Long sampleInMsgCount = msgSampleDs.count();
        //短信号码（样本）在短信中的覆盖率
        logger.error("短信号码（样本）在短信中的覆盖率:"+sampleInMsgCount+"/"+sampleCount+"="+sampleInMsgCount/sampleCount);

        //样本中（申请时间）往前推最晚（近）短信的时间减去申请时间（天数）分布
        Dataset<Row> submitTime_applicationDtDs = sc.sql("select md5No,datediff(to_date(max(submitTime)),to_date(first(applicationDt))) from sampleInfo where content != '' group by md5No");
        write2csv(submitTime_applicationDtDs,"最晚时间-申请时间");

        //样本中（申请时间）往前推最早（远）短信的时间减去申请时间（天数）分布
        Dataset<Row> applicationDt_submitTimeDs = sc.sql("select md5No,datedatediffdiff(to_date(first(applicationDt)),to_date(min(submitTime))) from sampleInfo where content != '' group by md5No");
        write2csv(applicationDt_submitTimeDs,"申请时间-最早时间");





    }


    protected static void write2csv(Dataset<Row> ds, String fileName){
        HdfsUtil.deleteFile("/result/"+fileType+"/"+fileName);
        ds.write().csv("hdfs://10.0.1.95:9000/result/"+fileType+"/"+fileName);
    }
    protected static  Dataset<Row>  getTelRdd(JavaSparkContext jsc, SQLContext sc,String hdfsHost, String sourcePath,String fileName){
        JavaRDD<String> lines = jsc.textFile(hdfsHost+sourcePath+fileName);

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

    protected static Dataset<Row> getMsgRdd(JavaSparkContext jsc, SQLContext sc,String hdfsHost, String sourcePath,String fileName){
        JavaRDD<String> msgLines = jsc.textFile(hdfsHost+sourcePath+fileName);

        //读取原始短信
        JavaRDD<Object> msgRdd = msgLines.map(new Function<String, Object>() {
            @Override
            public Message call(String msg) throws Exception {
                Message message = new Message();

                int idIndex = msg.lastIndexOf(",");
                String id = msg.substring(idIndex+2,msg.length()-1);
                msg = msg.substring(0,idIndex);
                int sidIndex = msg.lastIndexOf(",");
                String sid = msg.substring(sidIndex+2,msg.length()-1);
                msg = msg.substring(0,sidIndex);
                String[] msgInfo = msg.split(",");
                String serviceId = msgInfo[0].substring(1,+msgInfo[0].length()-1);
                String tel = msgInfo[1].substring(1,+msgInfo[1].length()-1);
                String submitTime = msgInfo[2].substring(1,+msgInfo[2].length()-1);
                String content = msg.substring(msgInfo[0].length()+msgInfo[1].length()+msgInfo[2].length()+4,msg.length()-1);

                message.setContent(content);
                message.setSid(sid);
                message.setSubmitTime(submitTime);
                message.setTel(tel);
                return message;
            }
        });

        Dataset<Row> msgDf = sc.createDataFrame(msgRdd, Message.class);
//        msgDf.registerTempTable("message");

        return msgDf;
    }

    protected static Dataset<Row> getMsgTagRdd(JavaSparkContext jsc,SQLContext sc,String hdfsHost,String sourcePath,String fileName){
        JavaRDD<String> msgTagLines = jsc.textFile(hdfsHost+sourcePath + fileName);

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

        Dataset<Row> msgTagDf = sc.createDataFrame(msgTagRdd, MessageTag.class).distinct();
        msgTagDf.registerTempTable("msgTag");

        return msgTagDf;
    }

    private static Dataset<Row> getSampleRdd(JavaSparkContext jsc,SQLContext sc,String hdfsHost,String sourcePath,String fileName){
        JavaRDD<String> lines = jsc.textFile(hdfsHost+sourcePath + fileName);

        JavaRDD<Object> sampleRdd = lines.map(new Function<String, Object>() {
            @Override
            public Object call(String line) throws Exception {
                String[] sampleArr = line.split(",");

                SampleInfo sample = new SampleInfo();
                SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy/MM/dd");
                SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
                sample.setMobile(sampleArr[0]);
                try{
                    sample.setApplicationDt(sdf2.format(sdf1.parse(sampleArr[1])));
                }catch (Exception e){
                    sample.setApplicationDt("1960-01-01");
                    e.printStackTrace();
                }
                sample.setOverdueDays(sampleArr[2]);
                return sample;
            }
        });

        Dataset<Row> sampleDs = sc.createDataFrame(sampleRdd, SampleInfo.class);
        sampleDs.registerTempTable("sampleInfo");


        return sampleDs;

    }
}
