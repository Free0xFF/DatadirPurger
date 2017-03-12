package cn.com.purger.server.persistence;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileSnap {
	private final File snapShotDir;
	
	public FileSnap(File snapShotDir) {
		this.snapShotDir = snapShotDir;
	}
	
	/*
	 * 找到最近n个最新的文件，最近的定义，就是zxid大的
	 * 条件：
	 * 1. snapshot开头
	 * 2. 最近n个版本
	 * 
	 */
	public List<File> findNRecentSnapShots(int num) {
		// 首先获取目录所有的文件，然后对文件按照zxid降序排列
		List<File> files = Utils.sortDataDirFiles(this.snapShotDir.listFiles(), "snapshot", false);
		List<File> fileList = new ArrayList<File>();
		int i = 0;
		for(File f : files) {
			if(i == num) {
				break;
			}
			fileList.add(f);
			i++;
		}
		return fileList;
	}
}
