import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.junit.Test;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * @author TaoQingYang
 * @date 2022/10/15
 */
public class HdfsTest {

    @Test
    public void demo1() throws IOException {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        InputStream inputStream = new URL("hdfs://taoqingyang:8020/b.txt").openStream();
        System.out.println(inputStream.available());
        FileOutputStream outputStream = new FileOutputStream(new File("D:\\hello.txt"));
        FileWriter fileWriter = new FileWriter("D:\\hello.txt");
        byte[] arr = new byte[1024];
//        while(inputStream.available() != 0){
//            inputStream.read(arr);
//            outputStream.write(arr);
//
//        }
//        fileWriter.write("hello world");
//        fileWriter.flush();


        IOUtils.copy(inputStream, fileWriter);

        IOUtils.closeQuietly(inputStream);

        IOUtils.closeQuietly(fileWriter);
//        fileWriter.close();
//        outputStream.flush();
//        inputStream.close();
//        outputStream.close();
    }
}
