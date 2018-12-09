package util;

import com.sou.MessageAnalysis.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class HdfsUtil {
    private static String profile = Config.profile;
    private static Logger logger = Logger.getLogger(HdfsUtil.class);
    private static Configuration conf = new Configuration();
    private static FileSystem hdfs;

    public HdfsUtil() {
    }

    public static FileStatus[] readFile(String pathString) {
        FileStatus[] fileStatuses = null;

        try {
            fileStatuses = hdfs.listStatus(new Path(pathString));
        } catch (IOException var3) {
            logger.error(var3.getMessage());
            var3.printStackTrace();
        }

        return fileStatuses;
    }

    public static boolean writeFile(String writePath, File tempFile) {
        try {

            FSDataOutputStream out = hdfs.create(new Path(writePath));
            InputStream in = new FileInputStream(tempFile);
            IOUtils.copyBytes(in, out, 4096, true);
            tempFile.delete();
            return true;
        } catch (IOException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    public static boolean deleteFile(String writePath){
        boolean deleteFlag = false;
        try {
            deleteFlag =  hdfs.delete(new Path(writePath));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return deleteFlag;
    }

    public static FileSystem getHdfs() {
        return hdfs;
    }

    public static Configuration getConf() {
        return conf;
    }

    static {
        PropertiesUtil.loadProperties(profile+"/config.properties");
        conf.set("fs.defaultFS", "10.0.1.95:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        try {
            hdfs = FileSystem.get(conf);
        } catch (IOException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }

    }
}