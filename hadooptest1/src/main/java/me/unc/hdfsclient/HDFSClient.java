package me.unc.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSClient {

    private FileSystem fs;

    @Before
    public void setUp() throws Exception {
        //获取HDFS对象
        fs = FileSystem.get(URI.create("hdfs://master:9000"), new Configuration(), "lzs22");
        System.out.println("connected to hdfs");
    }

    @Test
    //上传文件
    public void put() throws IOException {
        //上传文件
        fs.copyFromLocalFile(new Path("f:\\learning/hadoop/task1/task4.txt"), new Path("/task1"));
        System.out.println("上传成功");
    }

    @Test
    //获取文件
    public void get() throws IOException {
        fs.copyToLocalFile(new Path("/111.txt"), new Path("f:\\test/hadoop/get"));
        System.out.println("获取文件成功");
    }

    @Test
    //重命名文件夹
    public void rename() throws IOException {
        fs.rename(new Path("/test1"), new Path("/test"));
        System.out.println("修改文件名成功");
    }

    @Test
    //删除文件
    public void delete() throws IOException {
        boolean del = fs.delete(new Path("/taskout2-1"), true);
        if (del) {
            System.out.println("删除成功");
        } else {
            System.out.println("删除失败");
        }
    }

    @Test
    //追加文件内容
    public void append() throws IOException {
        FSDataOutputStream append = fs.append(new Path("/111.txt"), 1024);
        FileInputStream open = new FileInputStream("f:\\test/hadoop/put/222.txt");
        IOUtils.copyBytes(open, append, 1024, true);
        System.out.println("文件内容追加成功");
    }

    @Test
    //获取文件信息，listStatus同时查文件和文件夹
    public void ls() throws IOException {
        FileStatus[] fileStatus = fs.listStatus(new Path("/"));
        for (FileStatus status : fileStatus) {
            if (status.isFile()) {
                System.out.println("这是一个文件：");
                System.out.println(status.getPath());
                System.out.println(status.getOwner());
                System.out.println(status.getLen());
                System.out.println(status.getPermission());
            } else { //除了文件和文件夹还有可能食软链接
                System.out.println("这是一个文件夹");
                System.out.println(status.getPath());
            }
        }
    }

    @Test
    //获取文件块信息，listFiles只查“文件”，文件夹不存在block属性
    public void listFile() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
        while (files.hasNext()) {
            LocatedFileStatus file =  files.next();
            System.out.println("============================");
            System.out.println(file.getPath());
            System.out.println("块信息：");
            BlockLocation[] blockLocations = file.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.print("块在：");
                for (String host : hosts) {
                    System.out.println(host + " ");
                }
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        //关闭HDFS
        fs.close();
        System.out.println("close hdfs");
    }

}
