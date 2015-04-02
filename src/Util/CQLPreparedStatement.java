package Util;

import com.datastax.driver.core.PreparedStatement;

public class CQLPreparedStatement {
//	public static String STATEMENT_STRING_CREATE_TABLE_NBH = 
//			"CREATE TABLE IF NOT EXISTS mykeyspace.blobnbh (cv text primary key,fgprt blob,adjset set<text>);";
//	public static PreparedStatement STM_CREATE_TABLE_NBH = 
//			CassandraSession.getCassandraSession().prepare(STATEMENT_STRING_CREATE_TABLE_NBH);
	
	private static String STATEMENT_STRING_INSERT_INTO_NBH = 
			"INSERT INTO mykeyspace.blobnbh (cv, fgprt, adjset) VALUES (?,?,?);";
	public static PreparedStatement STM_INSERT_INTO_NBH = 
			CassandraSession.getCassandraSession().prepare(STATEMENT_STRING_INSERT_INTO_NBH);
	
	private static String STATEMENT_STRING_QUERY_NBH = 
			"SELECT * FROM mykeyspace.blobnbh WHERE cv=?;";
	public static PreparedStatement STM_QUERY_NBH = 
			CassandraSession.getCassandraSession().prepare(STATEMENT_STRING_QUERY_NBH);
	
	/*
	 * CREATE TABLE IF NOT EXISTS mykeyspace.spstar 
	 * (sp text primary key,
	 * degmap map<text,int>);
	 * 
	 */
	private static String STATEMENT_STRING_QUERY_SP = 
			"SELECT * FROM mykeyspace.spstar WHERE sp=?;";
	public static PreparedStatement STM_QUERY_SP = 
			CassandraSession.getCassandraSession().prepare(STATEMENT_STRING_QUERY_SP);
	
	private static String STATEMENT_STRING_INSERT_INTO_SP = 
			"INSERT INTO mykeyspace.spstar (sp, degmap) VALUES (?,?);";
	public static PreparedStatement STM_INSERT_INTO_SP = 
			CassandraSession.getCassandraSession().prepare(STATEMENT_STRING_INSERT_INTO_SP);
}
