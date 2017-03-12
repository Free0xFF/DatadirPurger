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
	 * �ҵ����n�����µ��ļ�������Ķ��壬����zxid���
	 * ������
	 * 1. snapshot��ͷ
	 * 2. ���n���汾
	 * 
	 */
	public List<File> findNRecentSnapShots(int num) {
		// ���Ȼ�ȡĿ¼���е��ļ���Ȼ����ļ�����zxid��������
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
