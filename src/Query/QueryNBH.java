package Query;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;

import Util.CQLPreparedStatement;
import Util.CassandraSession;
import Util.NeighborhoodTableTuple;

public class QueryNBH {
	public NeighborhoodTableTuple query(String cv){
		
		ResultSet resultSet = CassandraSession.getCassandraSession()
				.execute(CQLPreparedStatement.STM_QUERY_NBH.bind(cv));
		if (resultSet == null)
			return null;
		NeighborhoodTableTuple NBHTuple = new NeighborhoodTableTuple();
		for(Row row: resultSet){
			NBHTuple.setAdjSet(row.getSet("adjset", String.class));
			NBHTuple.setFgPrt(Bytes.toHexString(row.getBytes("fgprt")));
			NBHTuple.setCentralVertexLable(row.getString("cv"));
			//Normally, there should be only one row. So, break is ok. 
			break;
		}
		
		return NBHTuple;
		
	}
}
