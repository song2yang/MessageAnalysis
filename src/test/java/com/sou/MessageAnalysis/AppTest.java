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

            String msg = "\"8253\",\"CF34E22818C97154179002F2E88CB5FE\",\"2017-08-28 14:01:08\",\"【美团点评】团购已付款28.1元至尾号8076的账户，3个工作日内到账，付款编号129613129，付款项目：“西安羊肉泡馍2人餐”28.1元\",\"20171106000426797002\",\"100007996873591\"";
        }

}
