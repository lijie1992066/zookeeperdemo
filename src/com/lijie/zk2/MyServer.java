package com.lijie.zk2;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class MyServer {
	
	private static final String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
	
	private static final int sessionTimeout = 2000;
	
	private static ZooKeeper zk = null;
	
	private static String group = "/server";
	
	public static void main(String[] args) throws Exception {
		
		//注册
		regServer("192.168.80.123");
		//执行任务
		business("192.168.80.123");
	}
	
	/**
	 * 获取zookeeper实例
	 * @return
	 * @throws Exception
	 */
	public static ZooKeeper getZookeeper() throws Exception {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				// 收到watch通知后的回调函数
				System.out.println("事件类型" + event.getType() + "，路径" + event.getPath());
				
				//因为监听器只会监听一次，这样可以一直监听,且只监听"/"目录
				try {
					zk.getChildren("/", true);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		return zk;
	}
	
	/**
	 * 注册到zookeeper
	 * @param ip
	 * @throws Exception
	 */
	public static void regServer(String ip) throws Exception {
		zk = getZookeeper();
		String create = zk.create(group + "/server", ip.getBytes(), Ids.OPEN_ACL_UNSAFE,
			CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(ip + " 上线了！" + ",存储路径:" + create);
	}
	
	/**
	 * 业务
	 * @throws Exception
	 */
	public static void business(String ip) throws Exception {
		System.out.println(ip + " 处理业务");
		Thread.sleep(Long.MAX_VALUE);
	}
}
