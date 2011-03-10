package test.index;

import java.util.List;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;

/**
 * 保存最近2次的CommitPoint
 * 
 *
 * @author wuhao
 */
public class KeepLastTwoCommitsDeletionPolicy implements IndexDeletionPolicy {

	/**
	 * Deletes all commits except the most recent one.
	 */
	public void onInit(List<? extends IndexCommit> commits) {
		// Note that commits.size() should normally be 1:
		onCommit(commits);
	}

	/**
	 * Deletes all commits except the most recent one.
	 */
	public void onCommit(List<? extends IndexCommit> commits) {
		// Note that commits.size() should normally be 2 (if not
		// called by onInit above):
		int size = commits.size();
		if (size <= 2)
			return;
		for (int i = 0; i < size - 2; i++) {
			commits.get(i).delete();
		}
	}
}
