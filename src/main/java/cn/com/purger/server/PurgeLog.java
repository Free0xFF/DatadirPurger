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
 * 该类主要实现目录的清理具体行为
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
		// 保留的日志版本数必须大于3
		if(snapCountToBeRetain < 3) {
			// 此处是被调用，因此抛异常比较好
			throw new IllegalArgumentException(COUNT_ERROR_MSG);
		}
		
		// 借助FileSnapLog获取最小的zxid
		FileSnapLog fileSnapLog = new FileSnapLog(dataLogDir, snapShotDir);
		List<File> nRecentFiles = fileSnapLog.findNRecentSnapShots(snapCountToBeRetain);
		retainNRecentSnapShots(fileSnapLog, nRecentFiles);
	}
	
	public static void retainNRecentSnapShots(FileSnapLog fileSnapLog, List<File> nRecentFiles) {
		if(nRecentFiles.size() == 0) {
			return;
		}
		
		//获取最小的zxid
		File file = nRecentFiles.get(nRecentFiles.size()-1); //按照zxid降序排列的
		final long leastZxidToBeRetain = Utils.getZxidFromFileName(file.getName(), PREFIX_SNAPSHOT);
		
		// 接下来是过滤目录中的文件，需要保留以 snapshot和log开头的，并且version大于等于leastZxidToBeRetain的日志
		class MyFileFilter implements FileFilter {
			private String prefix;
			public MyFileFilter(String prefix) {
				this.prefix = prefix;
			}
			
			/**
			 * 接受过滤的条件：
			 * 1. prefix开头
			 * 2. zxid大于等于leastZxidToBeRetain
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
		
		// 然后就是遍历目录了
		List<File> files = new ArrayList<File>(Arrays.asList(fileSnapLog.getDataLogDir().listFiles(new MyFileFilter(PREFIX_LOG))));
		files.addAll(Arrays.asList(fileSnapLog.getSnapShotDir().listFiles(new MyFileFilter(PREFIX_SNAPSHOT))));
		
		// 获取到待删除的文件句柄后，就可以删除啦
		for(File f : files) {
			System.out.println("Removing File: "+f.getName()+"\t"+DateFormat.getTimeInstance().format(f.lastModified()));
			if(!f.delete()) {
				System.out.println("Removing File "+f.getName()+" failed.");
			}
		}
	}
}
