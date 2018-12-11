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

            String msg = "\"91810\"," + //特服号
                    "\"5232E572256E8A3902B367EF50C518F0\"," + //md5
                    "\"2016-12-28 07:12:42\"," + //submitTime
                    "\"1,100元扣款失败，请用app或微信号主动还款。如有疑问，可微信、APP或者10101058电话咨询人工客服确认情况。如已还款，请忽略本通知。\"," + //content
                    "\"20180112000233316514\"," + // sid
                    "\"100005776442925\""; //id



        }

        public static void splitMsg(){

        }

}
