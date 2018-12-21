package com.sou.MessageAnalysis;

import com.sou.MessageAnalysis.bean.gy.VariableParam;
import com.sou.MessageAnalysis.company.GyFintech;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import util.HdfsUtil;
import util.PropertiesUtil;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 */
public class App {
    private static Logger logger = Logger.getLogger(App.class);

    private static String profile = "pro";
    private static final String fileType = "DE";
    private static String sparkMaster;
    private static String hdfsHost;
    private static String sourcePath;
    private static String gySourcePath;

    static {
        try{
            PropertiesUtil.loadProperties(profile + "/config.properties");
        }catch (Exception e){
            e.printStackTrace();
            logger.error(e.getMessage());
        }

        sparkMaster = PropertiesUtil.getProperty("spark.master");
        hdfsHost = PropertiesUtil.getProperty("hdfs.host");
        sourcePath = PropertiesUtil.getProperty("source.path");
        gySourcePath = PropertiesUtil.getProperty("source.gy.path");
    }

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("MessageAnalyse").setMaster(sparkMaster).set("spark.sql.crossJoin.enabled","true");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        SQLContext sc = new SQLContext(jsc);


//        GyFintech.msgStatistics(jsc,sc,hdfsHost,gySourcePath,logger);

        Integer[] days = new Integer[2];
//        days[0] = 7;
        days[1] = 360;
//        days[2] = 60;
//        days[3] = 90;
//        days[4] = 120;
//        days[5] = 150;
//        days[6] = 180;
//        days[7] = 270;
//        days[8] = 360;
//        days[9] = 720;

        String[] labels = new String[5];
        labels[0] = "loan_amount";
        labels[1] = "pay_amount";
        labels[2] = "cc_bill_amount";
        labels[3] = "payout_amount";
        labels[4] = "payin_amount";

        List<VariableParam> params = new ArrayList<>();

        String applicationDt = "2018-7-1";


        //样本手机号码md5文件（15873222574     BA1674B46C363EFFD6D8B0153699F164）
        // 大额 CJM_1129_DE 小额 CJM_1129_XE
        Dataset<Row> telDs = GyFintech.getTelRdd(jsc, sc, hdfsHost, gySourcePath,"CJM_1129_"+fileType+".txt").distinct();
//        Dataset<Row> telDs = GyFintech.getTelRdd(jsc, sc, hdfsHost, gySourcePath,"CJM_1129_DE_single.txt").distinct();
//        telDs = telDs.drop(telDs.col("originalNo"));

        telDs.cache();

        List<String> paths = new ArrayList<>();


        Dataset<Row> msgTagDs = GyFintech.getMsgTagRdd(jsc, sc, hdfsHost, gySourcePath,"zz_tag_"+fileType+".csv").distinct();
   //     Dataset<Row> msgTagDs = GyFintech.getMsgTagRdd(jsc, sc, hdfsHost, gySourcePath,"singleTel_teg.csv").distinct();
        msgTagDs.registerTempTable("msgTag");


        // 样本手机号码+短息标签临时表
        Dataset<Row> sampleTagDs = telDs.join(msgTagDs, msgTagDs.col("telMd5").equalTo(telDs.col("md5No")));
        sampleTagDs.registerTempTable("sampleTagTemp");

        sc.cacheTable("sampleTagTemp");

        sampleTagDs.cache();



        for (Integer day:days) {

            //全量短信
            Dataset<Row> allSampleDs = sc.sql("select sendTime,tagKey,tagVal,md5No as md5No1" +
                    ",datediff(to_date('"+applicationDt+"'),to_date(sendTime)) as dateDiff from sampleTagTemp where datediff(to_date('"+applicationDt+"'),to_date(sendTime)) between 0 and "+ day);
            allSampleDs.registerTempTable("sampleAll");
//            allSampleDs.repartition(1).write().option("header",true).csv("hdfs://10.0.1.95:9000/result/DE/"+day+"_allSampleDs");
            sc.cacheTable("sampleAll");

            for (String lable:labels){
                Dataset<Row> ds = GyFintech.derivedVarsByCondition(jsc, sc, hdfsHost, gySourcePath, telDs, day, applicationDt, 0, 24, lable);
                ds.repartition(1).write().option("header",true).csv(hdfsHost+"/result/DE/"+lable+"_"+day);
                paths.add("/result/DE/"+lable+"_"+day);
            }
            sc.uncacheTable("sampleAll");

        }

        GyFintech.mergeFile(paths,sc,hdfsHost+"/result/temp/");





        

        System.exit(1);
       // ds.show();
//        GyFintech.derivedVarsByCondition(jsc,sc,hdfsHost,gySourcePath,100,"2018-1-1",0,24,"loan_amount").show();


//      掌众数据统计
//      Weshare.statistics(jsc,sc,hdfsHost,sourcePath);




    }



}
