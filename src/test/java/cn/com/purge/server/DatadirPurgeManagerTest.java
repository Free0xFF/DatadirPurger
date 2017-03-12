package cn.com.purge.server;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import cn.com.purger.server.DatadirPurgeManager;

public class DatadirPurgeManagerTest {
	
	public static final String dataLogDir = "D:\\test";
	public static final String snapShotDir = "D:\\test";
	public static final String version = "version-";
	public static final int VERSION = 1;
	
	static class NumCounter {
		public static NumCounter numCounter = new NumCounter();
		private int count = -1;
		
		public int incrementCount() {
			return ++count;
		}
		
		public static NumCounter getInstance() {
			return numCounter;
		}
		
	}
	
	static class DataFileGenerator implements Runnable {
		private File dataLogDir;
		private File snapShotDir;
		
		public DataFileGenerator(File dataLogDir, File snapShotDir) throws Exception {
			this.dataLogDir = new File(dataLogDir, version + VERSION);
			this.snapShotDir = new File(snapShotDir, version + VERSION);
			
			if(!this.dataLogDir.exists()) {
				System.out.println("dataLogDir not exists...start to create...");
				if(!this.dataLogDir.mkdir()) {
					throw new Exception("dataLogDir create error!");
				}
			}
			
			if(!this.snapShotDir.exists()) {
				System.out.println("snapShotDir not exists...start to create...");
				if(!this.snapShotDir.mkdir()) {
					throw new Exception("snapShotDir create error!");
				}
			}
			
		}
		
		@Override
		public void run() {
			while(true) {
				int version = NumCounter.getInstance().incrementCount();
				String snapShotFileName = String.format("snapshot.%d", version);
				String dataLogFileName = String.format("log.%d", version);
				File dataLog = new File(this.dataLogDir, dataLogFileName);
				File snapShot = new File(this.snapShotDir, snapShotFileName);
				try {
					if(!dataLog.createNewFile()) {
						System.out.println("create file: "+dataLog.getName()+" failed!");
					}
					System.out.println("create file: "+dataLog.getName()+" succeeded!");
					
					if(!snapShot.createNewFile()) {
						System.out.println("create file: "+snapShot.getName()+" failed!");
					}
					System.out.println("create file: "+snapShot.getName()+" succeeded!");
					
					Thread.sleep(TimeUnit.SECONDS.toMillis(10));
					
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public static void main(String[] args) {
		File dataLog = new File(dataLogDir);
		File snapShot = new File(snapShotDir);
	
		try {
			DatadirPurgeManager dpm = new DatadirPurgeManager(dataLog, snapShot, 3, 60);
			dpm.start();
			
			DataFileGenerator dfg = new DataFileGenerator(dataLog, snapShot);
			Thread t = new Thread(dfg);
			t.start();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
