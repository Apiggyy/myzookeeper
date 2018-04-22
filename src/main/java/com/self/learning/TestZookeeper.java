package com.self.learning;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TestZookeeper {
    //同步互斥变量，用来阻塞等待ZooKeeper连接完成之后再进行ZooKeeper的操作命令
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private static String hosts = "192.168.30.102:2181,192.168.30.103:2181,192.168.30.104:2181";
    private ZooKeeper zk;

    @Before
    public void init() throws Exception {
        zk = new ZooKeeper(hosts, 300000, new Watcher(){
            // 监控所有被触发的事件
            public void process(WatchedEvent event) {
                System.out.println("已经触发了" + event.getType() + "事件！");
                if ( Event.KeeperState.SyncConnected == event.getState() ) {
                    //连接完成的同步事件，互斥变量取消，下面的阻塞停止，程序继续执行
                    connectedSemaphore.countDown();
                }
            }
        });
        //如果和ZooKeeper服务器的TCP连接还没完全建立，就阻塞等待
        connectedSemaphore.await();
    }

    @Test
    public void createZnode() throws IOException, KeeperException, InterruptedException {
        String path = zk.create("/root/child02", "test02".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }

    @Test
    public void updateZnode() throws KeeperException, InterruptedException {
        Stat stat = zk.setData("/root/child02", "test02_new".getBytes(), 0);
        System.out.println(stat.getVersion());
    }

    @Test
    public void getZnodeData() throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        byte[] data = zk.getData("/root/child02", true, stat);
        System.out.println(new String(data));
    }

    @Test
    public void createEZnode() throws IOException, KeeperException, InterruptedException {
        String path = zk.create("/root/child03", "test03".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println(path);
    }

    @Test
    public void createPersistentSequentialZnode() throws IOException, KeeperException, InterruptedException {
        String path = zk.create("/root/child04", "test04".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        System.out.println(path);
    }

    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren("/root", true);
        for (String child : children) {
            System.out.println(child);
        }
    }

    @Test
    public void deleteZnode() throws KeeperException, InterruptedException {
        zk.delete("/root/child040000000005", 0);
    }

    @Test
    public void testWatcher() throws KeeperException, InterruptedException {
        Stat stat = zk.setData("/root/child1", "test001".getBytes(), 0);
        System.out.println(stat.getVersion());
    }
}
