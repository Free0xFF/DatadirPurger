package cn.com.purger.server.persistence;

import java.io.File;

@SuppressWarnings("unused")
public class FileLog {
	private final File dataLogDir;
	public FileLog(File dataLogDir) {
		this.dataLogDir = dataLogDir;
	}
}
