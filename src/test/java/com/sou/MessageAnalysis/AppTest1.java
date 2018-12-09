package com.sou.MessageAnalysis;

import com.sou.MessageAnalysis.bean.Message;
import com.sou.MessageAnalysis.bean.MessageRule;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import util.HdfsUtil;


/**
 * Unit test for simple App.
 */
public class AppTest1
    {
        public static void main(String[] args) {
//            System.out.println(478/100000);
            SparkConf conf = new SparkConf().setAppName("MessageAnalyse")
//                .setMaster("spark://10.0.1.90:7077");
                    .setMaster("local");

//        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

            JavaSparkContext jsc = new JavaSparkContext(conf);

            SQLContext sc = new SQLContext(jsc);
            JavaRDD<String> lines = jsc.textFile("hdfs://10.0.1.90:9000/result/hitAppMessage");

            JavaRDD<Object> messsages = lines.map(new Function<String, Object>() {
                public Message call(String v1) throws Exception {
                    String[] msgValues = v1.split(",");
                    Message msg = new Message();
                    msg.setTel(msgValues[3]);
                    String appName = msgValues[0];
                    appName = appName.substring(1,appName.length()-1);
                    msg.setAppName(appName);
                    msg.setSubmitTime(msgValues[2]);
                    msg.setContent(msgValues[1]);

                    return msg;
                }
            });

            Dataset<Row> df = sc.createDataFrame(messsages, Message.class);

            df.registerTempTable("message");

            Dataset<Row> dataset = sc.sql("select appName, count(*) as hitAppCounts ,count(distinct tel) as userCounts from message group by appName");

            dataset.show();

            HdfsUtil.deleteFile("/result/dataset");
            dataset.write().csv("hdfs://10.0.1.90:9000/result/dataset");

        }

}
