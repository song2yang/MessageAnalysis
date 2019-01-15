package com.sou.MessageAnalysis;

import com.sou.MessageAnalysis.bean.MessageRule;
import com.sou.MessageAnalysis.company.GyFintech;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import util.HdfsUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Unit test for simple App.
 */
public class AppTest
    {
        static String hdfsHost = "hdfs://10.0.1.95:9000/";
        public static void main(String[] args) {
            SparkConf conf = new SparkConf().setAppName("MessageAnalyse").setMaster("local").set("spark.sql.crossJoin.enabled","true");

            JavaSparkContext jsc = new JavaSparkContext(conf);

            SQLContext sc = new SQLContext(jsc);


//            Dataset<Row> ds1 = sc.sparkSession().read().option("header", true).csv("/Users/souyouyou/Documents/totalDs.csv");
//            Dataset<Row> ds2 = sc.sparkSession().read().option("header", true).csv("/Users/souyouyou/Documents/totalDs1.csv");


            Dataset<Row> ds1 = GyFintech.getTelRdd(jsc, sc, hdfsHost, "/data/gy/", "CJM_1129_DE_test.txt");
            Dataset<Row> ds2 = GyFintech.getSampleRdd(jsc, sc, hdfsHost, "/data/gy/", "YB_DE_test.csv");
            Dataset<Row> ds3 = ds1.join(ds2, ds1.col("originalNo").equalTo(ds2.col("mobile"))).drop(ds1.col("originalNo")).drop(ds2.col("mobile"));

            Dataset<Row> msgDs = GyFintech.getMsgTagRdd(jsc, sc, hdfsHost, "/data/gy/", "zz_tag_DE_test.csv");

            Dataset<Row> ds4 = ds3.join(msgDs, ds3.col("md5No").equalTo(msgDs.col("telMd5"))).drop(ds3.col("md5No"));
//            ds4.repartition(1).write().option("header",true).csv("/Users/souyouyou/Desktop/cloud/sample");

            ds4.registerTempTable("sample");


            Dataset<Row> ds5 = sc.sql("select telMd5,tagKey,count(tagKey) as tagCnt from sample group by telMd5,tagKey");

            Dataset<Row> ds6 = sc.sql("select telMd5,count(*)  as totalCnt from sample group by telMd5");

            Dataset<Row> ds7 = ds5.join(ds6, ds5.col("telMd5").equalTo(ds6.col("telMd5"))).drop(ds5.col("telMd5"))
                    .withColumn("tagPer",ds5.col("tagCnt").divide(ds6.col("totalCnt")));


            ds7.registerTempTable("temp");
            Dataset<Row> ds8 = sc.sql("select * from temp");
            Dataset<Row> ds9 = sc.sql("select telMd5, -sum(tagPer*log2(tagPer)) as entropy from temp group by telMd5");

            ds8.join(ds9,ds8.col("telMd5").equalTo(ds9.col("telMd5")),"left_outer").drop(ds9.col("telMd5"))
                    .withColumn("normal",ds9.col("entropy").divide(ds8.col("tagCnt"))).show();


        }






        




}
