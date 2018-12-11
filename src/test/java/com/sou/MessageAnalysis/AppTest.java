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


/**
 * Unit test for simple App.
 */
public class AppTest
    {
        public static void main(String[] args) {

            String msg = "\"300205\",\"B619AAF6963B1C4D4CB187D66234D0F1\",\"2015-03-31 17:42:48\",\",共提现110.80元\",\"20180116001891148451\",\"3683825959\"";
            String[] split = msg.split(",");

            for (String str:split) {
                System.out.println(str);
            }
        }

}
