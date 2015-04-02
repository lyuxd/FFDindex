package CreateDB;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import Util.CQLPreparedStatement;
import Util.SpStarIndexTuple;

import com.datastax.driver.core.Session;

public class CreateSpStarIndex extends CreateIndexAbstract {

	// Session cs = CassandraSession.getCassandraSession();

	@Override
	public boolean loadData(Session cs, String file_location) {
		// TODO Auto-generated method stub
		BufferedReader br;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(
					file_location)));
			String line = null;
			while ((line = br.readLine()) != null) {
				this.insertTupleIntoIndex(cs, line);
			}
			br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public boolean insertTupleIntoIndex(Session cs, String tuple) {
		// TODO Auto-generated method stub
		SpStarIndexTuple spStarIndexTuple = new SpStarIndexTuple();
		spStarIndexTuple.init(tuple);
			cs.execute(CQLPreparedStatement.STM_INSERT_INTO_SP.bind(
					spStarIndexTuple.getSp(), spStarIndexTuple.getDegMap()));
		
		return true;
	}

}
