package com.sou.MessageAnalysis;

import com.sou.MessageAnalysis.bean.gy.VariableParam;
import com.sou.MessageAnalysis.company.GyFintech;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.columnar.INT;
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

    public static void main(String[] argsr) {

        SparkConf conf = new SparkConf().setAppName("MessageAnalyse").setMaster(sparkMaster).set("spark.sql.crossJoin.enabled","true");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        SQLContext sc = new SQLContext(jsc);


//        GyFintech.msgStatistics(jsc,sc,hdfsHost,gySourcePath,logger);

        List<Integer> days = new ArrayList();
//        days.add(7);
//        days.add(30);
//        days.add(60);
//        days.add(90);
//        days.add(120);
//        days.add(150);
//        days.add(180);
//        days.add(270);
//        days.add(360);
        days.add(720);

        List<String> amtLabels = new ArrayList();
        amtLabels.add("loan_amount");
        amtLabels.add("pay_amount");
        amtLabels.add("cc_bill_amount");
        amtLabels.add("payout_amount");
        amtLabels.add("payin_amount");

        List<String> generalLabels = new ArrayList();
        generalLabels.add("label_loan_reg");
        generalLabels.add("label_loan_apl");
        generalLabels.add("label_loan_apl_succ");
        generalLabels.add("label_loan_apl_fail");
        generalLabels.add("label_loan_loan");
//        generalLabels.add("loan_amount");
        generalLabels.add("label_loan_pay");
//        generalLabels.add("pay_amount");
        generalLabels.add("label_loan_odue");
        generalLabels.add("label_loan_ad");
        generalLabels.add("label_loan_p2p");
        generalLabels.add("label_loan_xd");
        generalLabels.add("label_loan_xjfq");
        generalLabels.add("label_loan_xffq");
        generalLabels.add("label_loan_ccdh");
        generalLabels.add("label_loan");
        generalLabels.add("label_consume_hotel");
        generalLabels.add("label_consume_tour");
        generalLabels.add("label_consume_del");
        generalLabels.add("label_consume_ad");
        generalLabels.add("label_consume_order");
        generalLabels.add("label_consume");
        generalLabels.add("label_cc_buy");
        generalLabels.add("label_cc_bill");
        generalLabels.add("label_cc_ad");
//        generalLabels.add("cc_bill_amount");
        generalLabels.add("label_cc");
        generalLabels.add("label_consume_pay");
        generalLabels.add("label_consume_payout");
//        generalLabels.add("payout_amount");
        generalLabels.add("label_consume_payin");
//        generalLabels.add("payin_amount");
        generalLabels.add("label_virtual");
        generalLabels.add("label_invest");

        List<String> times = new ArrayList();
//        times.add("0_6");
//        times.add("6_11");
//        times.add("11_14");
//        times.add("14_17");
//        times.add("17_21");
//        times.add("21_24");
        times.add("0_24");

        String applicationDt = "2018-7-1";


        //样本手机号码md5文件（15873222574     BA1674B46C363EFFD6D8B0153699F164）
        // 大额 CJM_1129_DE 小额 CJM_1129_XE
        Dataset<Row> telDs = GyFintech.getTelRdd(jsc, sc, hdfsHost, gySourcePath,"CJM_1129_"+fileType+".txt").distinct();
//        Dataset<Row> telDs = GyFintech.getTelRdd(jsc, sc, hdfsHost, gySourcePath,"CJM_1129_DE_single.txt").distinct();
//        telDs = telDs.drop(telDs.col("originalNo"));

        telDs.cache();


        //样本原始文件（15961768685,2018/8/7,0）
        // 大额 YB_DE 小额 YB_XE
        Dataset<Row> sampleDs = GyFintech.getSampleRdd(jsc, sc, hdfsHost, gySourcePath, "YB_"+fileType+".csv").distinct();

        Dataset<Row> sampleTelDs = sampleDs.join(telDs, sampleDs.col("mobile").equalTo(telDs.col("originalNo"))).distinct();
        sampleTelDs = sampleTelDs.drop(sampleTelDs.col("originalNo")).drop(sampleTelDs.col("mobile"));


        List<String> paths = new ArrayList<>();


        Dataset<Row> msgTagDs = GyFintech.getMsgTagRdd(jsc, sc, hdfsHost, gySourcePath,"zz_tag_"+fileType+".csv");
   //     Dataset<Row> msgTagDs = GyFintech.getMsgTagRdd(jsc, sc, hdfsHost, gySourcePath,"singleTel_teg.csv").distinct();
        msgTagDs.registerTempTable("msgTag");


        // 样本手机号码+短息标签临时表
        Dataset<Row> sampleTagDs = sampleTelDs.join(msgTagDs, msgTagDs.col("telMd5").equalTo(sampleTelDs.col("md5No")));
        sampleTagDs.registerTempTable("sampleTagTemp");

        sc.cacheTable("sampleTagTemp");

        sampleTagDs.cache();



        for (Integer day:days) {

            for (String time:times){
                String beginTm = time.split("_")[0];
                String endTm = time.split("_")[1];


                //全量短信
                Dataset<Row> allSampleDs = sc.sql("select sendTime,tagKey,tagVal,md5No as md5No1 " +
                        "from sampleTagTemp where datediff(to_date('"+applicationDt+"'),to_date(sendTime)) between 0 and "+ day +
                        " and hour(sendTime) between "+beginTm+" and "+endTm );
                allSampleDs.registerTempTable("sampleAll");
//            allSampleDs.repartition(1).write().option("header",true).csv("hdfs://10.0.1.95:9000/result/DE/"+day+"_allSampleDs");
                sc.cacheTable("sampleAll");

                for (String label:amtLabels){
                    String tagLabel =  String.valueOf(day) + "_" + label + "_"+time;
                    Dataset<Row> ds = GyFintech.derivedAmtVars(sc, telDs, tagLabel, label);
                    ds = ds.dropDuplicates();
                    ds.repartition(1).write().option("header",true).csv(hdfsHost+"/result/DE/"+tagLabel);
                    paths.add(hdfsHost+"/result/DE/"+tagLabel);
                }


                Dataset<Row> countDs = sc.sql("select count(*) as CNT,md5No1 as md5No from sampleAll group by md5No1");
                for (String label:generalLabels) {
                    String tagLabel =  String.valueOf(day) + "_" + label + "_"+time;
                    Dataset<Row> ds = GyFintech.deriverdGeneralVars(sc, telDs, countDs, tagLabel, label);
                    ds.repartition(1).write().option("header",true).csv(hdfsHost+"/result/DE/"+tagLabel);
                    paths.add(hdfsHost+"/result/DE/"+tagLabel);
                }


                sc.uncacheTable("sampleAll");
            }

        }

        GyFintech.mergeFile(paths,sc,hdfsHost+"result/temp/");





        

        System.exit(1);
       // ds.show();
//        GyFintech.derivedVarsByCondition(jsc,sc,hdfsHost,gySourcePath,100,"2018-1-1",0,24,"loan_amount").show();


//      掌众数据统计
//      Weshare.statistics(jsc,sc,hdfsHost,sourcePath);




    }



}
