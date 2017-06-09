package cn.julong;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 */
public class LeaderElection {
    public static void main(String[] args) throws Exception {
        String connectString = "dpnode05:2181";
        new Master(connectString, connectString.substring(0, connectString.indexOf(":"))).start();
    }

    static class Master extends Thread{
        //心跳时间间隔
        private static final int heartbeatTime = 3000;
        private static final int sessionTimeOut = 10000;
        //等待上次作为leader的Master重连之前抢战leader前时间，可以防止网络波动导致leader频繁切换
        private static final int scrambleWaitTime = 5000;
        private static final String rootPath = "/wcm2";
        private static final String leaderPath = rootPath + "/leader";
        private ZooKeeper zk;
        private String learder;
        private String self;
        private Watcher watcher;
        private volatile boolean leaderDeadForever = true;
        private volatile boolean watcherConsumed = true;

        @Override
        public void run() {
            List<String> children = new ArrayList<>();
            while (true) {
                try {
                    //检测是否需要执行等待原来主节点重连
                    if (!leaderDeadForever && !learder.equals(self)) {
                        System.out.println("检测到主节点挂掉，等待" +(scrambleWaitTime/1000)+ "秒！...");
                        try { Thread.sleep(scrambleWaitTime); } catch (InterruptedException e1) { e1.printStackTrace(); }
                        leaderDeadForever = true;
                    }
                    //如果注册的监听器被消费，则从新注册监听器
                    if (watcherConsumed) {
                        //1. 注册监听器，坚挺根节点删除事件，如果坚挺到事件，立即进入第2条，并从新注册监听
                        zk.getChildren(rootPath, watcher).size();
                        watcherConsumed = false;
                    }

                    heartbeatAction();
                    //心跳结束，则输出本次主从状态
                    System.out.println(this.toString());
                    Thread.sleep(heartbeatTime);
                } catch (Exception e){
                    e.printStackTrace();
                }finally {
                }
            }
        }

        private void heartbeatAction() {
            while (true) {
                try {
                    //2. 尝试在根节点创建leader节点，创建成功，则写入自己的信息，心跳结束
                    zk.create(leaderPath, self.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    this.learder = self;
                    break;
                }catch (KeeperException.NodeExistsException e) {
                    //3. 创建失败，读取节点信息，读取成功，则记录leader节点信息，心跳结束
                    try {
                        byte[] leaderData = zk.getData(leaderPath, true, null);
                        learder = new String(leaderData);
                        break;
                    } catch (KeeperException.NoNodeException ee) {
                        //4. 读取失败，则立即返回第2条
                        if (null != learder && !learder.equals(self)) {
                            System.out.println("检测到主节点挂掉，等待" +(scrambleWaitTime/1000)+ "秒！...");
                            try { Thread.sleep(scrambleWaitTime); } catch (InterruptedException e1) { e1.printStackTrace(); }
                        }
                        continue;
                    } catch (Exception ee) {
                        ee.printStackTrace();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public Master(String connectString, String self) {
            super(self);
            this.learder = self;
            this.self = self;
            Thread thisThread = this;
            try {
                zk = new ZooKeeper(connectString, sessionTimeOut, null);
                watcher = new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        try {
                            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                                if (zk.getChildren(rootPath, watcher).size() == 0) {
                                    leaderDeadForever = false;
                                    watcherConsumed = true;
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                };
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public String toString() {
            return "Server{" +
                    "learder='" + learder + '\'' +
                    ", self='" + self + '\'' +
                    '}';
        }
    }
}
