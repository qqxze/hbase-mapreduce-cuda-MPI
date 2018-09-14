package one;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * @author ZhuQichao
 * @create 2018/5/22 19:04
 **/
public class hbase {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void main(String[] args) throws IOException {
        String tableName = "SC_java";
        String[] fileds = {"Basic:SC_Sno", "Basic:SC_Cno", "Score:Math", "Score:Computer Science", "Score:English"};
        String[] fileds1 = {"Basic:SC_Sno", "Basic:SC_Cno", "Score:Math", "Score:English"};
        String[] values = {"2015001", "123001", "86", "69"};
        init();
        createTable(tableName, fileds);

        System.out.println("******************************* before addRecord  ********************************");
        scanTable(tableName);
        System.out.println("******************************* start addRecord  ********************************");
        addRecord(tableName, "Zhangsan", fileds1, values);
        System.out.println("******************************* after addRecord  ********************************");
        scanTable(tableName);

        System.out.println("******************************* scanColumn  ********************************");
        scanColumn(tableName, "Zhangsan", "Score:Math");

        System.out.println("******************************* modifyData  ********************************");
        modifyData(tableName, "Zhangsan", "Score:Math", "100");
        System.out.println("******************************* after modifyData  ********************************");
        scanColumn(tableName, "Zhangsan", "Score:Math");
        System.out.println("******************************* deleteRow  ********************************");
        deleteRow(tableName, "Zhangsan");
        System.out.println("******************************* after deleteRow  ********************************");
        scanTable(tableName);

        deleteTable(tableName);
        close();

    }

    public static void init() {
        configuration = HBaseConfiguration.create();
        //configuration.set("hbase.rootdir","hdfs://172.31.42.151:9000/hbase");
        /*configuration.set("hbase.zookeeper.quorum", "cluster-152,cluster-87,cluster-214");*/
        configuration.set("hbase.zookeeper.quorum", "172.31.42.152,172.31.42.87,172.31.42.214");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            System.out.println("初始化成功");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void close() {
        if (admin != null) {
            try {
                admin.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (null != connection) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("关闭连接成功");
    }

    public static void createTable(String tableName, String[] fileds) throws IOException {
        TableName tableName1 = TableName.valueOf(tableName);
        if (admin.tableExists(tableName1)) {
            System.out.println("table exists!");
            deleteTable(tableName);
        }
        Set<String> colFamily = new HashSet<>();
        for (int i = 1; i < fileds.length; i++) {
            colFamily.add(fileds[i].split(":")[0]);
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName1);
        for (Iterator<String> it = colFamily.iterator(); it.hasNext(); ) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(it.next().toString());
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
        System.out.println("create " + tableName + " success!");

    }

    public static void addRecord(String tableName, String row, String[] fileds, String[] values) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < fileds.length; i++) {
            String colFamily = fileds[i].split(":")[0];
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(fileds[i].split(":")[1]), Bytes.toBytes(values[i]));
            puts.add(put);
        }
        table.put(puts);
        System.out.println("insert row : " + row + " success!");
        table.close();
    }

    public static void scanColumn(String tableName, String row, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(column.split(":")[0]), Bytes.toBytes(column.split(":")[1]));
        ResultScanner resultScanner = table.getScanner(scan);
        long count = 0;
        for (Result r : resultScanner) {
            System.out.println("Scan Row : " + row);
            for (KeyValue kv : r.list()) {

                System.out.println("colfamily : " + Bytes.toString(kv.getFamily()));
                System.out.println("col : " + Bytes.toString(kv.getQualifier()));
                if (kv.getValueLength() == 0) {
                    System.out.println("value : " + "null");
                } else {
                    System.out.println("value : " + Bytes.toString(kv.getValue()));
                }

                count++;
            }
            System.out.println("the total scan number is : " + count);
            System.out.println("Scan end ！ ");
        }
        table.close();
        System.out.println();

    }

    public static void scanTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result r : resultScanner) {
            if (r.isEmpty()) {
                System.out.println("NO data Now ");
            } else {
                for (KeyValue kv : r.list()) {
                    System.out.println("Scan Row : " + Bytes.toString(kv.getRow()));
                    System.out.println("colfamily : " + Bytes.toString(kv.getFamily()));
                    System.out.println("col : " + Bytes.toString(kv.getQualifier()));
                    if (kv.getValueLength() == 0) {
                        System.out.println("value : " + "null");
                    } else {
                        System.out.println("value : " + Bytes.toString(kv.getValue()));
                    }
                }
            }
        }

        System.out.println("Scan end ！ ");
        table.close();
        System.out.println();

    }

    public static void modifyData(String tableName, String row, String column, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        String colFamily = column.split(":")[0];
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(column.split(":")[1]), Bytes.toBytes(value));

        table.put(put);
        System.out.println("modify  rowKey : " + row + " column : " + column + " success!");
        table.close();

    }

    public static void deleteRow(String tableName, String row) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> deletes = new ArrayList<Delete>();
        Delete delete = new Delete(Bytes.toBytes(row));
        deletes.add(delete);
        table.delete(deletes);
        System.out.println("delete rowKey : " + row + "success!");

    }

    public static void deleteTable(String tableName) {
        try {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));

                admin.deleteTable(TableName.valueOf(tableName));
                System.out.println("delete " + tableName + " success!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
