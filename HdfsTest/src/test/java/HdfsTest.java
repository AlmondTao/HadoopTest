import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
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

    @Test
    public void getFileSystem1() throws IOException {
        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFS","hdfs://taoqingyang:8082");

        FileSystem fileSystem = FileSystem.get(configuration);

        System.out.println("fileSystem:"+fileSystem.toString());

    }

    @Test
    public void getFileSystem2() throws IOException, URISyntaxException {

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://taoqingyang:8082"), new Configuration());

        System.out.println("fileSystem"+fileSystem.toString());

    }


    @Test
    public void getFileSystem3() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://taoqingyang:8082");

        FileSystem fileSystem = FileSystem.newInstance(configuration);

        System.out.println("fileSystem"+fileSystem.toString());


    }

    @Test
    public void getFileSystem4() throws URISyntaxException, IOException {

        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://taoqingyang:8082"), new Configuration());

        System.out.println("fileSystem"+fileSystem.toString());

    }


    @Test
    public void listMyFiles() throws URISyntaxException, IOException {

        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://taoqingyang:8082"), new Configuration());

        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);

        while(locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            System.out.println(next.getPath().toString());
        }

        fileSystem.close();
    }

    @Test
    public void mkdirs() throws URISyntaxException, IOException {

        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://taoqingyang:8082"), new Configuration());

        boolean mkdirs = fileSystem.mkdirs(new Path("/javaTest/mkdir/test"));

        fileSystem.close();
    }


    @Test
    public void getFileToLocal() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://taoqingyang:8082"), new Configuration());

        FSDataInputStream open = fileSystem.open(new Path("/b.txt"));

        FileOutputStream outputStream = new FileOutputStream(new File("D:\\b.txt"));

        IOUtils.copy(open,outputStream);
        IOUtils.closeQuietly(open);
        IOUtils.closeQuietly(outputStream);

        fileSystem.close();

    }


    @Test
    public void putData() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://taoqingyang:8082"), new Configuration());

        fileSystem.copyFromLocalFile(new Path("file:///D:\\b.txt"),new Path("/javaTest/mkdir/test"));

        fileSystem.close();
    }


    @Test
    public void mergeFile() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs:/taoqingyang:8082"), new Configuration());

        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/big.xml"));


        LocalFileSystem local = FileSystem.getLocal(new Configuration());

        FileStatus[] fileStatuses = local.listStatus(new Path("file:///E:\\input"));

        for (FileStatus localFile : fileStatuses){
            FSDataInputStream inputStream = local.open(localFile.getPath());
            IOUtils.copy(inputStream,fsDataOutputStream);
            IOUtils.closeQuietly(inputStream);

        }

        IOUtils.closeQuietly(fsDataOutputStream);
        local.close();
        fileSystem.close();

    }




}
