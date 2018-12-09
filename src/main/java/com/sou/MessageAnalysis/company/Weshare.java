package com.sou.MessageAnalysis.company;

import com.sou.MessageAnalysis.bean.Message;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import util.LabelUtil;
import util.SparkUtil;

import java.util.List;

public class Weshare {
    public static void statistics(JavaSparkContext jsc, SQLContext sc, String hdfsHost, String sourcePath){
        Column[] colArr = {new Column("app_name"),new Column("code"),new Column("value")} ;
//        Dataset appDs = readMySQL(sc, "app", new Column("app_name"));
        Dataset appDs = SparkUtil.readMySQL(sc, "app_zzcf", new Column("app_name"));

        Dataset ruleDs = SparkUtil.readMySQL(sc, "rule",colArr);


        final List ruleList = ruleDs.collectAsList();
        final List appList = appDs.collectAsList();

        JavaRDD<String> lines = jsc.textFile(hdfsHost+sourcePath);
        //短信app打标签
        JavaRDD<Object> messages = LabelUtil.markMsgApp(lines, appList);

        Dataset<Row> df = sc.createDataFrame(messages, Message.class);
        df.registerTempTable("message");


        df.show();

        //        //命中平台名称的短信
//        Dataset<Row> hitAppDs = sc.sql("select *  from message where appName != 'N'");
//
//        SparkUtil.writeCsv("/result/hitAppMessage",hitAppDs);
//
//        //命中平台+规则的短信
//        Dataset<Row> hitAppMsgDs = sc.read().csv(hdfsHost+"/result/hitAppMessage");
//        JavaRDD<Object> hitMsgRuleRdd = LabelUtil.markMsgrule(hitAppMsgDs.toJavaRDD(), ruleList);
//
//        Dataset<Row> hitMsgRuleDs = sc.createDataFrame(hitMsgRuleRdd, Message.class);
//        SparkUtil.writeCsv("/result/hitMsgRule",hitMsgRuleDs);
//
//        hitMsgRuleDs.registerTempTable("messageRule");

        //统计用户注册平台数
        // Dataset<Row> halfOfYearAppCounts = sc.sql("select tel ,count(distinct appName) as appCount  from message where appName != 'N' and submitTime between '2017-04-01' and '2017-10-01' group by tel");
        //Dataset<Row> yearAppCounts = sc.sql("select tel ,count(distinct appName) as appCount  from message where appName != 'N' group by tel");

//        HdfsUtil.deleteFile("/result/hitAppTel");
//        sc.sql("select distinct tel from message where appName != 'N'").write().csv("hdfs://10.0.1.95:9000/result/hitAppTel");




        //HdfsUtil.deleteFile("/result/yearAppCounts");
        // yearAppCounts.write().csv("hdfs://10.0.1.95:9000/result/yearAppCounts");

//        HdfsUtil.deleteFile("/result/hitAppCount");
//        sc.sql("select collect_set(appName) as AppName,count(*) as HitAppCount,count(distinct tel) as HitAppUserCount from message where appName != 'N' group by appName")
//                .write().json("hdfs://10.0.1.95:9000/result/hitAppCount");
//
//
//       Dataset<Row> hitAppMsgDs = sc.read().csv("hdfs://10.0.1.95:9000/result/hitAppMessage");
//        JavaRDD<Row> hitAppMsgRdd = hitAppMsgDs.toJavaRDD();



      /*  Dataset<Row> hitMsgRuleDs = sc.createDataFrame(hitMsgRuleRdd, Message.class);
        HdfsUtil.deleteFile("/result/hitMsgRule");
        hitMsgRuleDs.write().csv("hdfs://10.0.1.95:9000/result/hitMsgRule");
        hitMsgRuleDs.registerTempTable("messageRule");


        HdfsUtil.deleteFile("/result/ZC");
        sc.sql("select distinct tel from messageRule where hitRuleCode = '注册'")
                .write().csv("hdfs://10.0.1.95:9000/result/ZC");

        HdfsUtil.deleteFile("/result/YQ");
        sc.sql("select distinct tel from messageRule where hitRuleCode = '逾期'")
                .write().csv("hdfs://10.0.1.95:9000/result/YQ");


        HdfsUtil.deleteFile("/result/ZC_YQ");
        sc.sql("select distinct tel from messageRule where hitRuleCode = '注册' and tel not in (select distinct tel from messageRule where hitRuleCode = '逾期' )")
                .write().csv("hdfs://10.0.1.95:9000/result/ZC_YQ");*/

        /* HdfsUtil.deleteFile("/result/hitAppUsersAndAppName");
        sc.sql("select distinct tel,appName from messageRule")
            .write().csv("hdfs://10.0.1.95:9000/result/hitAppUsersAndAppName");

        HdfsUtil.deleteFile("/result/hitAppCount");
        sc.sql("select collect_set(appName) as AppName,count(*) as HitAppCount,count(distinct tel) as HitAppUserCount from messageRule group by appName")
                 .write().json("hdfs://10.0.1.95:9000/result/hitAppCount");

        HdfsUtil.deleteFile("/result/hitRuleCount");
        sc.sql("select collect_set(appName) as AppName,count(*) as HitRuleCount,count(distinct tel) as HitAppUserCount from messageRule where isHit = 1 group by appName")
                .write().json("hdfs://10.0.1.95:9000/result/hitRuleCount");

         */


//        hitMsgRuleDs.show();
//
//        HdfsUtil.deleteFile("/result/hitMsgRule");
//        hitMsgRuleDs.write().text("hdfs://10.0.1.95:9000/result/hitMsgRule");
    }
}
