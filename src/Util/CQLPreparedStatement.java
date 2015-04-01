package Util;

import com.datastax.driver.core.PreparedStatement;

public class CQLPreparedStatement {
	private static String STATEMENT_STRING_INSERT_INTO_NBH = 
			"INSERT INTO mykeyspace.blobnbh (cv, fgprt, adjset) VALUES (?,?,?);";
	public static PreparedStatement STM_INSERT_INTO_NBH = 
			CassandraSession.getCassandraSession().prepare(STATEMENT_STRING_INSERT_INTO_NBH);
}
