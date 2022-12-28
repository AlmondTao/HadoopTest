import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;

public class HBaseDMLTest {

    private static Connection connection;

    static {
        Configuration configuration = HBaseConfiguration.create();

        configuration.set("hbase.zookeeper.quorum","node01,node02,node03");
        configuration.set("hbase.zookeeper.property.clientPort","2181");

        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void putData(String tableName,String rowKey,String cf,String cn,String data) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));




        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(data));
        table.put(put);

        table.close();




    }


    public static void getData(String tableName,String rowKey,String cf,String cn) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
//        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        Result result = table.get(get);

        Cell[] cells = result.rawCells();
        for (Cell cell:cells){
            System.out.println("ROW:"+ Bytes.toString(CellUtil.cloneRow(cell))
            +"\nCF:"+Bytes.toString(CellUtil.cloneFamily(cell))
            +"\nCL:"+Bytes.toString(CellUtil.cloneQualifier(cell))
            +"\nVALUES:"+Bytes.toString(CellUtil.cloneValue(cell)));

        }

        table.close();


    }

    public static void deletedData(String tableName,String rowKey) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(Bytes.toBytes(rowKey));
        
        table.delete(delete);

    }

    public static void main(String[] args) throws IOException {
//        putData("apiTest:Student","1001","info","name","zhangsan");
//        putData("apiTest:Student","1001","info","age","18");
        putData("apiTest:Student","1002","info","name","lisi");
        putData("apiTest:Student","1002","info","age","20");
//        putData("apiTest:Student","1003","info","name","wangwu");
//        putData("apiTest:Student","1003","info","age","22");

        putData("apiTest:Student","1002","familyInfo","fName","lisibaba");
        putData("apiTest:Student","1002","familyInfo","mName","lisimama");
        putData("apiTest:Student","1002","familyInfo","fName","lisibaba2");
        putData("apiTest:Student","1002","familyInfo","mName","lisimama2");

//        getData("apiTest:Student","1001","","");

        deletedData("apiTest:Student","1002");
    }
}
