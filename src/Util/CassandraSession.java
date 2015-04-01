package Util;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;


public class CassandraSession {
	private static Cluster cluster;
	private static Session session;
	private static Metadata metadata;
	public static Session createCassandraSession(String add, String port){
		cluster = Cluster.builder().addContactPoint(add).build();
		
		metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for(Host host: metadata.getAllHosts()){
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n", 
					host.getDatacenter(),
					host.getAddress(),
					host.getRack());
		}
		session = cluster.connect();
		return session;
	}
	public static Session getCassandraSession(){
		
		return session;
	}
	public static void close(){
		cluster.close();
	}
}
