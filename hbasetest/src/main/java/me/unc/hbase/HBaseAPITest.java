package me.unc.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseAPITest {

    private static HBaseAdmin admin;
    private static Configuration configuration;
//    private Connection connection;

    static {
        //初始化
        try {
            //1.获取配置文件信息
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");

            //2.获取管理员对象(旧API)
            // 新API: (创建一个私有Connection)
            // Connection connection = ConnectionFactory.createConnection(configuration);
            // Admin admin = connection.getAdmin();
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 判断表是否存在
     *
     * @param tableName 表名
     * @return
     * @throws IOException
     */
    public static boolean isTableExists(String tableName) throws IOException {

        //1.判断表是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        //2.返回
        return exists;
    }

    /**
     * 创建表
     *
     * @param tableName 表名
     * @param cfs       列族
     */
    public static void createTable(String tableName, String... cfs) throws IOException {
        //1.判断表是否存在列族信息
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息！");
            return;
        }

        //2.判断表是否存在
        if (isTableExists(tableName)) {
            System.out.println("表已存在！");
            return;
        }

        //3.创建表描述器（HTableDescriptor）
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        //4.循环添加列族信息
        for (String cf : cfs) {
            //5.创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);


            //6.添加具体的列族信息
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        //7.创建表
        admin.createTable(hTableDescriptor);

    }

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    public static void dropTable(String tableName) throws IOException {
        //1.判断表是否存在
        if (!isTableExists(tableName)) {
            System.out.println(tableName + "表不存在！");
            return;
        }

        //2.使表下线
        admin.disableTable(TableName.valueOf(tableName));

        //3.删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * 创建命名空间
     *
     * @param nameSpace 命名空间
     */
    public static void createNameSpace(String nameSpace) {
        //1.创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();

        //2.创建命名空间
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            System.out.println("命名空间已存在！！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 向表插入数据
     *
     * @param tableName 表名
     * @param rowKey    行关键字
     * @param cf        列族
     * @param cn        列名
     * @param value     值
     */
    public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {
        //1.获取table对象
        //新API（1.x以上） ：
        //Table table = connection.getTable(TableName.valueOf(tableName));
        HTable table = new HTable(configuration, tableName);

        //2.创建Put对象，传入rowKey的直接数组
        //Put对象可创建不存在的列名
        Put put = new Put(Bytes.toBytes(rowKey));

        //3.给put对象添加信息
        //新API（1.x以上）使用addColumn()
        put.add(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));

        //4.插入数据
        table.put(put);

        //5.关闭资源
        table.close();
    }

    /**
     * 获取数据（get）
     *
     * @param tableName 表名（必要）
     * @param rowKey    行关键字（必要）
     * @param cf        列族
     * @param cn        列名
     */
    public static void getData(String tableName, String rowKey, String cf, String cn) throws IOException {
        //1.获取table对象
        //新API（1.x以上） ：
        //Table table = connection.getTable(TableName.valueOf(tableName));
        HTable table = new HTable(configuration, tableName);

        //2.创建get对象,传入rowKey
        Get get = new Get(Bytes.toBytes(rowKey));

        //2.1指定获取详细数据
        if (cf != null && cn == null) {
            //指定列族
            get.addFamily(Bytes.toBytes(cf));
        }
        if (cf != null && cn != null) {
            //指定列族和列
            get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        }

        //2.2设置获取数据的版本数
        get.setMaxVersions();

        //3.获取数据
        Result result = table.get(get);

        //4.解析result
        for (Cell cell : result.rawCells()) {
            //5.打印数据
            System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                    ", CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    ", CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    ", Value:" + Bytes.toString(CellUtil.cloneValue(cell)) +
                    ", TS:" + cell.getTimestamp());
        }

        //5.关闭表连接
        table.close();
    }

    /**
     * 获取数据（scan）
     *
     * @param tableName 表名
     */
    public static void scanTable(String tableName) throws IOException {
        //1.获取table对象
        //新API（1.x以上） ：
        //Table table = connection.getTable(TableName.valueOf(tableName));
        HTable table = new HTable(configuration, tableName);

        //2.创建scan对象，可以设置现在rowKey范围
        Scan scan = new Scan();

        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析resultScanner
        for (Result result : resultScanner) {
            //5.解析result
            for (Cell cell : result.rawCells()) {
                //6.打印数据
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        ", CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ", CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ", Value:" + Bytes.toString(CellUtil.cloneValue(cell)) +
                        ", TS:" + cell.getTimestamp());
            }
        }

        //5.关闭表连接
        table.close();
    }

    public static void deleteData(String tableName, String rowKey, String cf, String cn) throws IOException {
        //1.获取table对象
        //新API（1.x以上） ：
        //Table table = connection.getTable(TableName.valueOf(tableName));
        HTable table = new HTable(configuration, tableName);

        //2.创建delete对象
        //只传入rowKey相当于deleteall操作
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //2.1指定删除列（复杂！）
        /*
        delete.deleteColumn(); -> 删除最新Version数据（添加删除标记）
        delete.deleteColumns(); -> 删除所有Version的数据  （推荐使用）
         */
        //！！注意，可以指定时间戳
        if (cf != null && cn != null) {
            delete.deleteColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));
        }

        //3.执行删除操作
        table.delete(delete);

        //4.关闭表连接
        table.close();
    }

    /**
     * 关闭资源
     */
    public static void close() {

        if (admin != null) {
            try {
                //关闭连接
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //新API，关闭Connection
        /*
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
         */

    }

    public static void main(String[] args) throws IOException {

        //1.判断表是否存在 TODO
//        System.out.println(isTableExists("stu"));

        //2.创建表测试
//        createTable("stu", "1163", "1164");

//        System.out.println(isTableExists("stu"));

        //3.删除表测试
//        dropTable("stu");

        //4.创建命名空间测试
//        createNameSpace("1163");

        //5.创建数据测试
//        putData("stu", "a614", "1163", "name", "ljj");

        //6.获取数据测试 TODO
//        getData("stu", "1163", null, null);

        //7.扫描表测试
//        scanTable("stu");

        //8.删除数据测试
//        deleteData("stu", "1163", null, null);

        //关闭资源
        close();
    }

}
