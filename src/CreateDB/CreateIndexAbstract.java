package CreateDB;

import com.datastax.driver.core.Session;

import Util.IndexTupleAbstract;

public abstract class CreateIndexAbstract {
	public abstract boolean loadData(Session cs, String file_location);
	public abstract boolean insertTupleIntoIndex(Session cs, String tuple);
}
