package cn.com.purger.server.persistence;

import java.io.File;
import java.util.List;

public interface SnapShot {
	public List<File> findNRecentSnapShots(int num);
}
