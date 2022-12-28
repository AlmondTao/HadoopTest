import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseDDLTest {

    private static Connection connection;

    static{
        Configuration configuration = HBaseConfiguration.create();

        configuration.set("hbase.zookeeper.quorum","node01,node02,node03");
        configuration.set("hbase.zookeeper.property.clientPort","2181");

        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean isTableExist(String tableName) throws IOException {



        Admin admin = connection.getAdmin();

        boolean b = admin.tableExists(TableName.valueOf(tableName));
        return b;

    }

    public static void createTable(String nameSpace,String tableNameStr,String... cfs) throws IOException {
        if (cfs.length < 0){
            System.out.println("请设置列族信息");
            return;
        }

        if (isTableExist(tableNameStr)){
            System.out.println("需要创建的表已存在");
        }

        Admin admin = connection.getAdmin();
//        TableName tableName = TableName.valueOf(nameSpace, tableNameStr);
        TableName tableName = TableName.valueOf(tableNameStr);

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);

        tableDescriptorBuilder.setDurability(Durability.ASYNC_WAL);
        int version = 1;
        for (String cf : cfs){
            ColumnFamilyDescriptor build = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).setMaxVersions(version).build();
            version++;

            tableDescriptorBuilder.setColumnFamily(build);
        }


        admin.createTable(tableDescriptorBuilder.build());

        admin.close();


    }

    public static void dropTable(String tableName) throws IOException {
        if (!isTableExist(tableName)){
            System.out.println("删除的表不存在");
            return;
        }

        Admin admin = connection.getAdmin();

        admin.disableTable(TableName.valueOf(tableName));

        admin.deleteTable(TableName.valueOf(tableName));

        admin.close();

    }

    public static void createNameSpace(String ns) throws IOException {
        Admin admin = connection.getAdmin();

        NamespaceDescriptor build = NamespaceDescriptor.create(ns).build();

        try {
            admin.createNamespace(build);
        } catch (NamespaceExistException e){
            System.out.println("命名空间已存在");
        }finally {
            admin.close();
        }




    }



    public static void main(String[] args) throws IOException {
//        createNameSpace("apiTest");
//        dropTable("apiTest:Student");
        createTable("","apiTest:Student","info","familyInfo");

    }
}
