package cn.com.purger.server;

import java.io.File;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.com.purger.server.persistence.FileSnapLog.DataDirException;

/**
 * ������Ҫά��һ����ʱ���̣߳�ִ�ж�������
 * 
 * @author yongmao.gui
 *
 */
public class DatadirPurgeManager {
	public static final Logger LOG = LoggerFactory.getLogger(DatadirPurgeManager.class);
	
	private final File dataLogDir; //������־Ŀ¼��log.version
	private final File snapShotDir; //������־Ŀ¼��snapshot.version
	private final int snapCountToBeRetain; //����İ汾����
	private final int purgeInterval; //��ʱ����ļ����Сʱ��
	private PurgeTaskStatus purgeTaskStatus = PurgeTaskStatus.NOT_STARTED;
	private Timer timer;
	
	public enum PurgeTaskStatus {
			NOT_STARTED, STARTED, COMPLETED
	}
	
	public DatadirPurgeManager(File dataLogDir, File snapShotDir, int snapCountToBeRetain, int purgeInterval) {
		this.dataLogDir = dataLogDir;
		this.snapShotDir = snapShotDir;
		this.snapCountToBeRetain = snapCountToBeRetain;
		this.purgeInterval = purgeInterval;
		LOG.info("parameter snapCountToBeRetain was set to "+snapCountToBeRetain);
		LOG.info("parameter purgeInterval was set to "+purgeInterval);
	}
	
	public void start() {
		if(PurgeTaskStatus.STARTED == purgeTaskStatus) {
			LOG.warn("Purge Task has been started, skip to start...");
			return;
		}
		
		// ��ϵ����ʱ���������ڴ˴��ж������Ϸ���
		if(purgeInterval <= 0) {
			LOG.warn("purgeInterval should be great than 0, skip to start...");
			return;
		}
		
		timer = new Timer("PurgeTask", true);
		TimerTask purgeTimeTask = new PurgeTimerTask(dataLogDir, snapShotDir, snapCountToBeRetain);
		timer.scheduleAtFixedRate(purgeTimeTask, 0, TimeUnit.SECONDS.toMillis(purgeInterval));
		purgeTaskStatus = PurgeTaskStatus.STARTED;
	}
	
	static class PurgeTimerTask extends TimerTask {
		private File dataLogDir;
		private File snapShotDir;
		private int snapCountToBeRetain;
		
		public PurgeTimerTask(File dataLogDir, File snapShotDir, int snapCountToBeRetain) {
			this.dataLogDir = dataLogDir;
			this.snapShotDir = snapShotDir;
			this.snapCountToBeRetain = snapCountToBeRetain;
		}
		
		@Override
		public void run() {
			LOG.info("start to purge...");
			try {
				System.out.println("start to purge...");
				PurgeLog.purge(dataLogDir, snapShotDir, snapCountToBeRetain);
			} catch (DataDirException e) {
				e.printStackTrace();
				LOG.error(e.getMessage());
			}
		}
	}
	
	public void shutdown() {
		if(PurgeTaskStatus.STARTED == purgeTaskStatus) {
			LOG.info("shutdown the purge task...");
			timer.cancel();
			purgeTaskStatus = PurgeTaskStatus.COMPLETED;
		}
		else {
			LOG.info("purge task not started, skip to shutdown...");
		}
	}
	
	public static void main(String[] args) throws InterruptedException {
		File dataLog = new File("d:\\test");
		File snapShot = new File("d:\\test");
		DatadirPurgeManager dpm = new DatadirPurgeManager(dataLog, snapShot, 3, 60);
		dpm.start();
		Thread.sleep(10000000);
	}
	
}
