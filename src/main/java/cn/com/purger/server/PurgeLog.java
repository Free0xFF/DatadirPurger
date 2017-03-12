package cn.com.purger.server;

import java.io.File;
import java.io.FileFilter;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.com.purger.server.persistence.FileSnapLog;
import cn.com.purger.server.persistence.FileSnapLog.DataDirException;
import cn.com.purger.server.persistence.Utils;

/**
 * ������Ҫʵ��Ŀ¼�����������Ϊ
 * 
 * @author yongmao.gui
 *
 */
public class PurgeLog {
	public static final Logger LOG = LoggerFactory.getLogger(PurgeLog.class);
	public static final String PREFIX_LOG = "log";
	public static final String PREFIX_SNAPSHOT = "snapshot";
	private static final String COUNT_ERROR_MSG = "Snapshot Count To Be Retain Must great than or equal 3...";
	
	
	public static void purge(File dataLogDir, File snapShotDir, int snapCountToBeRetain) throws DataDirException {
		// ��������־�汾���������3
		if(snapCountToBeRetain < 3) {
			// �˴��Ǳ����ã�������쳣�ȽϺ�
			throw new IllegalArgumentException(COUNT_ERROR_MSG);
		}
		
		// ����FileSnapLog��ȡ��С��zxid
		FileSnapLog fileSnapLog = new FileSnapLog(dataLogDir, snapShotDir);
		List<File> nRecentFiles = fileSnapLog.findNRecentSnapShots(snapCountToBeRetain);
		retainNRecentSnapShots(fileSnapLog, nRecentFiles);
	}
	
	public static void retainNRecentSnapShots(FileSnapLog fileSnapLog, List<File> nRecentFiles) {
		if(nRecentFiles.size() == 0) {
			return;
		}
		
		//��ȡ��С��zxid
		File file = nRecentFiles.get(nRecentFiles.size()-1); //����zxid�������е�
		final long leastZxidToBeRetain = Utils.getZxidFromFileName(file.getName(), PREFIX_SNAPSHOT);
		
		// �������ǹ���Ŀ¼�е��ļ�����Ҫ������ snapshot��log��ͷ�ģ�����version���ڵ���leastZxidToBeRetain����־
		class MyFileFilter implements FileFilter {
			private String prefix;
			public MyFileFilter(String prefix) {
				this.prefix = prefix;
			}
			
			/**
			 * ���ܹ��˵�������
			 * 1. prefix��ͷ
			 * 2. zxid���ڵ���leastZxidToBeRetain
			 * 
			 */
			@Override
			public boolean accept(File f) {
				if(!f.getName().startsWith(prefix+".")) {
					return false;
				}
				long fZxid = Utils.getZxidFromFileName(f.getName(), prefix);
				if(fZxid >= leastZxidToBeRetain) {
					return false;
				}
				return true;
			}
		}
		
		// Ȼ����Ǳ���Ŀ¼��
		List<File> files = new ArrayList<File>(Arrays.asList(fileSnapLog.getDataLogDir().listFiles(new MyFileFilter(PREFIX_LOG))));
		files.addAll(Arrays.asList(fileSnapLog.getSnapShotDir().listFiles(new MyFileFilter(PREFIX_SNAPSHOT))));
		
		// ��ȡ����ɾ�����ļ�����󣬾Ϳ���ɾ����
		for(File f : files) {
			System.out.println("Removing File: "+f.getName()+"\t"+DateFormat.getTimeInstance().format(f.lastModified()));
			if(!f.delete()) {
				System.out.println("Removing File "+f.getName()+" failed.");
			}
		}
	}
}
