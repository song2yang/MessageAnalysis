package com.sou.MessageAnalysis;

import com.sou.MessageAnalysis.bean.Message;
import com.sou.MessageAnalysis.bean.MessageRule;
import com.sou.MessageAnalysis.company.GyFintech;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
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
public class AppTest1
    {
        public static void main(String[] args) throws IOException {
            String hdfsHost = "hdfs://10.0.1.95:9000/";
//            System.out.println(478/100000);
            SparkConf conf = new SparkConf().setAppName("MessageAnalyse")
//                .setMaster("spark://10.0.1.90:7077");
                    .setMaster("local");

//        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

            JavaSparkContext jsc = new JavaSparkContext(conf);

            SQLContext sc = new SQLContext(jsc);
//            JavaRDD<String> lines = jsc.textFile("hdfs://10.0.1.90:9000/result/hitAppMessage");


            List<String> paths = new ArrayList<>();
//            paths.add(hdfsHost+"/result/XE/720_0_24_entropy");
//            paths.add(hdfsHost+"/result/XE/label_loan_apl_720_0_24_");
//            paths.add(hdfsHost+"/result/XE/label_loan_reg_720_0_24_");
//            paths.add(hdfsHost+"/result/XE/payin_amount_720_0_24_");
//            GyFintech.mergeFile(paths,sc,hdfsHost+"result/temp/");
//            Dataset<Row> ds = sc.sparkSession().read().option("header", true).csv("hdfs://10.0.1.95:9000/result/XE/totalDs");

//            Dataset<Row> sampleDs = GyFintech.getSampleRdd(jsc, sc, hdfsHost, "data/gy/", "YB_XE.csv").distinct();
            String label = "10_20_30";
            System.out.println(label.substring(label.indexOf("_")+1));

            FileStatus[] fileStatuses = HdfsUtil.getHdfs().listStatus(new Path("hdfs://10.0.1.95:9000/result/DE"));
            for (FileStatus fileStatus:fileStatuses){
                paths.add(fileStatus.getPath().toString());
            }
            System.out.println(fileStatuses.length);
//            Dataset<Row> sampleDs = GyFintech.getSampleRdd(jsc, sc, hdfsHost, "/data/gy", "YB_XE.csv").distinct();
//            GyFintech.mergeFile(paths,sc,"/result/temp",sampleDs);

        }

}
