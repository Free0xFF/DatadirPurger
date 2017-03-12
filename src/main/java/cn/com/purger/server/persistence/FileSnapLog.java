package cn.com.purger.server.persistence;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * 该类是一个帮助类，用来获取最近的n个文件
 * 该类同时也维护一个目录版本号，方便升级管理
 * 
 * @author yongmao.gui
 *
 */
@SuppressWarnings("unused")
public class FileSnapLog {
	public static final Logger LOG = Logger.getLogger(FileSnapLog.class);
	private File dataLogDir;
	private File snapShotDir;
	private FileLog fileLog;
	private FileSnap fileSnap;
	public static final String version = "version-";
	public static final int VERSION = 1;
	
	// 设置一个自动创建目录的参数，如果目录不存在，默认情况下程序可以自动生成目录
	public static final String DATADIR_AUTO_CREATE = "datadir.auto.create";
	public static final String DATADIR_AUTO_CREATE_DEFAULT = "true";
	
	public FileSnapLog(File dataLogDir, File snapShotDir) throws DataDirException {
		this.dataLogDir = new File(dataLogDir, version + VERSION);
		this.snapShotDir = new File(snapShotDir, version + VERSION);
		
		// 维护dataLogDir和snapShotDir，因此需要对参数的合法性进行验证
		Boolean enableAutoCreate = Boolean.parseBoolean(
				System.getProperty(DATADIR_AUTO_CREATE, DATADIR_AUTO_CREATE_DEFAULT));
		
		if(!this.dataLogDir.exists()) {
			LOG.warn("dataLogDir not exists...");
			if(!enableAutoCreate) {
				throw new DataDirException("Data Log dir not exists, please create manully...");
			}
			if(!this.dataLogDir.mkdir()) {
				throw new DataDirException("Auto create data log dir failed...");
			}
		}
		
		if(!this.snapShotDir.exists()) {
			LOG.warn("snapShotDir not exists...");
			if(!enableAutoCreate) {
				throw new DataDirException("SnapShot dir not exists, please create manully...");
			}
			if(!this.snapShotDir.mkdir()) {
				throw new DataDirException("Auto create snapshot dir failed...");
			}
		}
		
		this.fileLog = new FileLog(this.dataLogDir);
		this.fileSnap = new FileSnap(this.snapShotDir);
	}
	
	
	@SuppressWarnings("serial")
	public static class DataDirException extends Exception {
		public DataDirException(String msg) {
			super(msg);
		}
		
		public DataDirException(String msg, Exception e) {
			super(msg, e);
		}
	}
	
	public List<File> findNRecentSnapShots(int num) {
		FileSnap fSnap = new FileSnap(this.snapShotDir);
		List<File> files = fSnap.findNRecentSnapShots(num);
		return files;
	}
	
	public File getDataLogDir() {
		return this.dataLogDir;
	}
	
	public File getSnapShotDir() {
		return this.snapShotDir;
	}
	
	
}
