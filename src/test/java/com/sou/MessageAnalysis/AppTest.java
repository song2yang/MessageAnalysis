package com.sou.MessageAnalysis;

import com.sou.MessageAnalysis.bean.MessageRule;
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


            List<String> paths = new ArrayList<>();
            paths.add("/result/DE/cc_bill_amount_360");
            paths.add("/Users/souyouyou/Desktop/cloud/DE/cc_bill_amount_7");
            paths.add("/Users/souyouyou/Desktop/cloud/DE/loan_amount_360");
            paths.add("/Users/souyouyou/Desktop/cloud/DE/loan_amount_7");
            paths.add("/Users/souyouyou/Desktop/cloud/DE/pay_amount_360");
            paths.add("/Users/souyouyou/Desktop/cloud/DE/pay_amount_7");
            paths.add("/Users/souyouyou/Desktop/cloud/DE/payin_amount_360");
            paths.add("/Users/souyouyou/Desktop/cloud/DE/payin_amount_7");
            paths.add("/Users/souyouyou/Desktop/cloud/DE/payout_amount_360");
//            paths.add("/Users/souyouyou/Desktop/cloud/DE/payout_amount_7");
//            System.out.println("FINAL"+mergeFile(paths,sc));;
        }






        




}
