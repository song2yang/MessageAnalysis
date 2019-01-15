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


//        Dataset<Row> ds1 = sc.sparkSession().read().option("header", true).csv("hdfs://10.0.1.95:9000/result/DE/totalDs");
//        Dataset<Row> ds2 = sc.sparkSession().read().option("header", true).csv("hdfs://10.0.1.95:9000/result/DE/totalDs1");
//
//        ds1.join(ds2,ds1.col("md5No").equalTo(ds2.col("md5No"))).drop(ds1.col("md5No")).drop(ds1.col("originalNo")).repartition(1).write().option("header",true).csv("hdfs://10.0.1.95:9000/result/DE/result");
//
//
//        System.exit(1);
//        GyFintech.msgStatistics(jsc,sc,hdfsHost,gySourcePath,logger);

        List<Integer> days = new ArrayList();
        days.add(7);
        days.add(30);
        days.add(60);
        days.add(90);
        days.add(120);
        days.add(150);
        days.add(180);
        days.add(270);
        days.add(360);
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
        generalLabels.add("label_loan_pay");
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
        generalLabels.add("label_cc");
        generalLabels.add("label_consume_pay");
        generalLabels.add("label_consume_payout");
        generalLabels.add("label_consume_payin");
        generalLabels.add("label_virtual");
        generalLabels.add("label_invest");
        generalLabels.add("OTHER");

        List<String> times = new ArrayList();
        times.add("0_6");
        times.add("6_11");
        times.add("11_14");
        times.add("14_17");
        times.add("17_21");
        times.add("21_24");
        times.add("0_24");



        //样本手机号码md5文件（15873222574     BA1674B46C363EFFD6D8B0153699F164）
        // 大额 CJM_1129_DE 小额 CJM_1129_XE
//        Dataset<Row> telDs = GyFintech.getTelRdd(jsc, sc, hdfsHost, gySourcePath,"CJM_1129_"+fileType+".txt").distinct();
        Dataset<Row> telDs = GyFintech.getTelRdd(jsc, sc, hdfsHost, gySourcePath,"CJM_1129_"+fileType+"_test.txt").distinct();
//        Dataset<Row> telDs = GyFintech.getTelRdd(jsc, sc, hdfsHost, gySourcePath,"CJM_1129_DE_single.txt").distinct();
//        telDs = telDs.drop(telDs.col("originalNo"));

        telDs.cache();

        //样本原始文件（15961768685,2018/8/7,0）
        // 大额 YB_DE 小额 YB_XE
        Dataset<Row> sampleDs = GyFintech.getSampleRdd(jsc, sc, hdfsHost, gySourcePath, "YB_"+fileType+"_test.csv").distinct();
//        Dataset<Row> sampleDs = GyFintech.getSampleRdd(jsc, sc, hdfsHost, gySourcePath, "YB_"+fileType+".csv").distinct();
        sampleDs.cache();

        Dataset<Row> sampleTelDs = sampleDs.join(telDs, sampleDs.col("mobile").equalTo(telDs.col("originalNo"))).distinct();
        sampleTelDs = sampleTelDs.drop(sampleTelDs.col("originalNo")).drop(sampleTelDs.col("mobile"));

        List<String> paths = new ArrayList<>();


//        Dataset<Row> msgTagDs = GyFintech.getMsgTagRdd(jsc, sc, hdfsHost, gySourcePath,"zz_tag_"+fileType+"_test.csv");
        Dataset<Row> msgTagDs = GyFintech.getMsgTagRdd(jsc, sc, hdfsHost, gySourcePath,fileType+"_BASE_TAG.csv");
   //     Dataset<Row> msgTagDs = GyFintech.getMsgTagRdd(jsc, sc, hdfsHost, gySourcePath,"singleTel_teg.csv").distinct();
        msgTagDs.registerTempTable("msgTag");


        // 样本手机号码+短息标签临时表
        Dataset<Row> sampleTagDs = sampleTelDs.join(msgTagDs, msgTagDs.col("telMd5").equalTo(sampleTelDs.col("md5No")));
        sampleTagDs.registerTempTable("sampleTagTemp");

//
//        sampleTagDs.repartition(1).write().option("header",true).csv("hdfs://10.0.1.95:9000/data/gy/tag.csv");
//        System.exit(1);
        sc.cacheTable("sampleTagTemp");

        sampleTagDs.cache();



        for (Integer day:days) {

            for (String time:times){
                String beginTm = time.split("_")[0];
                String endTm = time.split("_")[1];


                //全量短信
                Dataset<Row> allSampleDs = sc.sql("select sendTime,tagKey,tagVal,md5No as md5No1,applicationDt " +
                        "from sampleTagTemp where datediff(to_date(applicationDt),to_date(sendTime)) between 0 and "+ day +
                        " and hour(sendTime) between "+beginTm+" and "+endTm );
                allSampleDs.registerTempTable("sampleAll");
//            allSampleDs.repartition(1).write().option("header",true).csv("hdfs://10.0.1.95:9000/result/DE/"+day+"_allSampleDs");
                sc.cacheTable("sampleAll");


                Dataset<Row> countDs = sc.sql("select count(*) as CNT,md5No1 as md5No from sampleAll group by md5No1");
                countDs.cache();

                String entropyLabel =   String.valueOf(day) + "_" + time+"_";
                Dataset<Row> tagKeyDs = sc.sql("select md5No1 as md5No,tagKey,count(tagKey) as tagCnt from sampleAll group by md5No1,tagKey");

                Dataset<Row> totalCntDs = sc.sql("select md5No1 as md5No,count(*)  as totalCnt from sampleAll group by md5No1");

                totalCntDs = telDs.join(totalCntDs,telDs.col("md5No").equalTo(totalCntDs.col("md5No")),"left_outer").drop(totalCntDs.col("md5No"));

                tagKeyDs.join(totalCntDs, tagKeyDs.col("md5No").equalTo(totalCntDs.col("md5No"))).drop(tagKeyDs.col("md5No"))
                        .withColumn("tagPer", tagKeyDs.col("tagCnt").divide(totalCntDs.col("totalCnt"))).drop(totalCntDs.col("totalCnt"))
                        .registerTempTable(entropyLabel+"temp");

                Dataset<Row> entropyDs = sc.sql("select md5No, -sum(tagPer*log2(tagPer)) as " + entropyLabel + "entropy from " + entropyLabel + "temp group by md5No");

                totalCntDs.join(entropyDs,totalCntDs.col("md5No").equalTo(entropyDs.col("md5No")),"left_outer").drop(totalCntDs.col("md5No"))
                        .withColumn(entropyLabel+"normal",entropyDs.col(entropyLabel + "entropy").divide(totalCntDs.col("totalCnt"))).drop(totalCntDs.col("totalCnt"))
                        .repartition(1).write().option("header",true).csv(hdfsHost+"/result/"+fileType+"/"+entropyLabel+"entropy");

                paths.add(hdfsHost+"/result/"+fileType+"/"+entropyLabel+"entropy");


                for (String label:amtLabels){
                    String tagLabel =  label + "_"+String.valueOf(day) + "_" + time + "_";
                    Dataset<Row> ds = GyFintech.derivedAmtVars(sc, telDs, countDs, tagLabel, label);
                    ds = ds.dropDuplicates();
                    ds.repartition(1).write().option("header",true).csv(hdfsHost+"/result/"+fileType+"/"+tagLabel);
                    paths.add(hdfsHost+"/result/"+fileType+"/"+tagLabel);
                }

                for (String label:generalLabels) {
                    String tagLabel =  label + "_"+String.valueOf(day) + "_" + time + "_";
                    Dataset<Row> ds = GyFintech.deriverdGeneralVars(sc, telDs, countDs, tagLabel, label);
                    ds = ds.dropDuplicates();
                    ds.repartition(1).write().option("header",true).csv(hdfsHost+"/result/"+fileType+"/"+tagLabel);
                    paths.add(hdfsHost+"/result/"+fileType+"/"+tagLabel);
                }


                sc.uncacheTable("sampleAll");
            }

        }

        GyFintech.mergeFile(paths,sc,hdfsHost+"result/temp/",sampleDs);





        

        System.exit(1);
       // ds.show();
//        GyFintech.derivedVarsByCondition(jsc,sc,hdfsHost,gySourcePath,100,"2018-1-1",0,24,"loan_amount").show();


//      掌众数据统计
//      Weshare.statistics(jsc,sc,hdfsHost,sourcePath);




    }



}
