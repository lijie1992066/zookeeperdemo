package com.lijie.zk2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;

public class MyClient {
	
	private static final String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
	
	private static final int sessionTimeout = 2000;
	
	private static ZooKeeper zk = null;
	
	private static String group = "/server";
	
	private static volatile List<String> ipList = null;
	
	private static CountDownLatch cdl = new CountDownLatch(1);
	
	public static void main(String[] args) throws Exception {
		
		//获取列表并且监听
		getList();
		if (!(States.CONNECTED == zk.getState())) {
			try {
				cdl.await();
			} catch (InterruptedException e) {
				throw new IllegalStateException(e);
			}
		}
		//执行业务
		business();
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
				
				if (event.getState() == KeeperState.SyncConnected) {
					cdl.countDown();
				}
				
				// 收到watch通知后的回调函数
				System.out.println("事件类型" + event.getType() + "，路径" + event.getPath());
				//重新更新列表，并注册监听
				try {
					getList();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		
		return zk;
	}
	
	/**
	 * 获取服务器列表，并监听父节点下面的变化
	 * @return
	 * @throws Exception
	 */
	public static void getList() throws Exception {
		zk = getZookeeper();
		//获取节点名字
		List<String> children = zk.getChildren(group, true);
		//声明装载服务ip的集合
		ArrayList<String> ips = new ArrayList<String>();
		
		for (String path : children) {
			//获取数据
			byte[] data = zk.getData(group + "/" + path, false, null);
			ips.add(new String(data));
		}
		ipList = ips;
		//打印服务器列表
		System.out.println("打印服务器列表" + ipList);
	}
	
	/**
	 * 业务
	 * @throws Exception
	 */
	public static void business() throws Exception {
		System.out.println("客户端处理");
		Thread.sleep(Long.MAX_VALUE);
	}
	
}
