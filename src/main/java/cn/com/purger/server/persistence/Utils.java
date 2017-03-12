package cn.com.purger.server.persistence;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Utils {
	
	/**
	 * �ļ����� snapshot.version, log.version
	 * �����ļ�����ȡaxid, ��������һ���汾��
	 * @param name
	 * @param prefix
	 * @return
	 */
	public static long getZxidFromFileName(String name, String prefix) {
		long zxid = -1;
		try {
			String[] nameParts = name.split("\\.");
			if(nameParts.length == 2 && nameParts[0].equals(prefix)) {
				zxid = Long.parseLong(nameParts[1], 16);
			}
		}
		catch(NumberFormatException e) {
		}
		
		return zxid;
	}
	
	@SuppressWarnings("serial")
	static class DataDirFileNameComparator implements Comparator<File>, Serializable {
		private String prefix;
		private Boolean ascending;
		
		public DataDirFileNameComparator(String prefix, Boolean ascending) {
			this.prefix = prefix;
			this.ascending = ascending;
		}
		
		@Override
		public int compare(File f1, File f2) {
			long fZxid1 = Utils.getZxidFromFileName(f1.getName(), prefix);
			long fZxid2 = Utils.getZxidFromFileName(f2.getName(), prefix);
			int result = (fZxid1<fZxid2) ? (-1) : (fZxid1>fZxid2 ? 1 : 0);
			return ascending ? result : -result;
		}
		
	}
	
	/**
	 * ����������
	 * ��������������úܶ࣬���������С�ĺϷ��Զ�����Ҫ��FileSnap�н����ж���
	 * @param files
	 * @param prefix
	 * @param ascending
	 * @return
	 */
	public static List<File> sortDataDirFiles(File[] files, String prefix, Boolean ascending) {
		if(files == null) {
			return new ArrayList<File>(0);
		}
		else {
			List<File> filesToSort = Arrays.asList(files);
			Collections.sort(filesToSort, new DataDirFileNameComparator(prefix, ascending));
			return filesToSort;
		}
	}
}
