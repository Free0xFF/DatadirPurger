package cn.com.purger.server.persistence;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * ������һ�������࣬������ȡ�����n���ļ�
 * ����ͬʱҲά��һ��Ŀ¼�汾�ţ�������������
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
	
	// ����һ���Զ�����Ŀ¼�Ĳ��������Ŀ¼�����ڣ�Ĭ������³�������Զ�����Ŀ¼
	public static final String DATADIR_AUTO_CREATE = "datadir.auto.create";
	public static final String DATADIR_AUTO_CREATE_DEFAULT = "true";
	
	public FileSnapLog(File dataLogDir, File snapShotDir) throws DataDirException {
		this.dataLogDir = new File(dataLogDir, version + VERSION);
		this.snapShotDir = new File(snapShotDir, version + VERSION);
		
		// ά��dataLogDir��snapShotDir�������Ҫ�Բ����ĺϷ��Խ�����֤
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
