package CreateDB;

import java.util.HashSet;
import java.util.Set;

import Util.CQLPreparedStatement;
import Util.CassandraSession;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class CreateNBH {

	/*
	 * This class is to generate the neighborhood table. The input file is like:
	 * 
	 * ent5 0x34daf 0#ent2#lab3<>1#ent9#lab7 ... Each line represents an
	 * star-shaped subgraph. In which the first element is the label of central
	 * vertex, the second fingerprint, and the last the adjacent edges of
	 * central vertex.
	 */

	public boolean create(String file_location) {

		Session cs = CassandraSession.getCassandraSession();
		BufferedReader br;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(
					file_location)));
			String line = null;
			while ((line = br.readLine()) != null) {
				System.out.println(line);
				this.insertIntoBlobNBH(cs, line);
			}
			br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

		}
		return false;

	}

	// input example: ent5 0x34daf 0#ent2#lab3<>1#ent9#lab7
	/**
	 * 
	 * @param session
	 * This is the session which will execute the cql.
	 * @param record
	 * Line read from local file.
	 */
	public void insertIntoBlobNBH(Session session, String record) {
		String[] elements = record.split(" |\t");
		if (elements.length != 3) {
			return;
		}

		Set<String> edgeset = new HashSet<String>();
		String[] adjedges = elements[2].split("<>");
		for (String edge : adjedges) {
			edgeset.add(edge);
		}


		BoundStatement stmt = CQLPreparedStatement.STM_INSERT_INTO_NBH.bind(elements[0],
				Bytes.fromHexString(elements[1]), edgeset);
		session.execute(stmt);
		try {

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
