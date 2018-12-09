package util;

import com.sou.MessageAnalysis.Config;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Properties;

public class SparkUtil {
    private static String profile = Config.profile;
    private static String hdfsHostAddr = PropertiesUtil.getProperty("hdfs.host");

    public static void writeCsv(String filePath, Dataset<Row> ds){
        HdfsUtil.deleteFile(filePath);
        ds.write().csv(hdfsHostAddr+"/"+filePath);

    }

    public static void writeJson(String filePath, Dataset<Row> ds){
        HdfsUtil.deleteFile(filePath);
        ds.write().json(hdfsHostAddr+"/"+filePath);

    }

    public static Dataset readMySQL(SQLContext sqlContext, String table, Column... col) {
        String url = "jdbc:mysql://10.0.1.95:3306/ym";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "root");
        connectionProperties.put("driver", "com.mysql.jdbc.Driver");

        Dataset<Row> dataset = sqlContext.read().jdbc(url, table, connectionProperties).select(col);
        //显示数据
        return dataset;
    }
}
