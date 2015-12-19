import hdfs.web.util.HDFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;
import org.springframework.format.number.NumberFormatter;

import java.io.IOException;
import java.util.Locale;

public class HDFSTest {


    private String formatNumber(Number num, String pattern) {
        if (num == null) {
            return null;
        }
        NumberFormatter formatter = new NumberFormatter();
        formatter.setPattern(pattern);
        return formatter.print(num, Locale.SIMPLIFIED_CHINESE);
    }

@Test
    public void test() throws IOException {
        Configuration conf = new Configuration();
        String newDir = "/test";
        // 01 test of if the path is exist 
        if (HDFSUtil.exist(conf, newDir)) { //??
            System.out.println(newDir + " already exist!");
        } else {
            //02 test of create directory
            boolean result = HDFSUtil.createDirectory(conf, newDir);
            if (result) {
                System.out.println(newDir + " create success!");
            } else {
                System.out.println(newDir + " create fail!");
            }
        }
        String fileContent = "Hi,hadoop. I love you";
        String newFileName = newDir + "/myfile.txt";

        //03 test of create file
        HDFSUtil.createFile(conf, newFileName, fileContent);
        System.out.println(newFileName + "create success!");

        //04 test of read the content of a file
        System.out.println(newFileName + " content is:\n" + HDFSUtil.readFile(conf, newFileName));

        //05 test of fetch all information of directories  
        FileStatus[] dirs = HDFSUtil.listStatus(conf, "/");
        System.out.println("--All sub_directory are as followed ---");
        for (FileStatus s : dirs) {
            System.out.println(s);
        }

        //06.test of fetch all files
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> files = HDFSUtil.listFiles(fs, "/", true);
        System.out.println("---All files are as followed---");
        while (files.hasNext()) {
            System.out.println(files.next());
        }
        fs.close();

        //test of delete file
        boolean isDeleted = HDFSUtil.deleteFile(conf, newDir);
        System.out.println(newDir + "delete success!");
	    }
}
