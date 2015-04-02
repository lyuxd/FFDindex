package Query;

//import com.datastax.driver.core.Result;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import Util.CQLPreparedStatement;
import Util.CassandraSession;
import Util.SpStarIndexTuple;
import Util.IndexTupleAbstract;

public class QuerySpIndex extends QueryIndex {
	//this.Super();
	
	
	@Override
	public IndexTupleAbstract query(String key) {
		// TODO Auto-generated method stub
		//Session sc = CassandraSession.getCassandraSession();
		ResultSet result  = CassandraSession.getCassandraSession()
				.execute(CQLPreparedStatement.STM_QUERY_SP.bind(key));
		SpStarIndexTuple spStarIndexTuple = new SpStarIndexTuple();
		for(Row row : result){
			//System.out.println(row.getString(1));
			spStarIndexTuple.setSp(row.getString("sp"));
			spStarIndexTuple.setDegMap(row.getMap("degmap", String.class, Integer.class));
			//Normally, only one tuple is retrieved. So break is ok.
			break;
		}
		return spStarIndexTuple;
	}

}
