package ffdIndex;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.omg.CORBA.PUBLIC_MEMBER;

public class Util {
	private static final TTransport tr = new TSocket("1.2.3.103", 9160);
	
public enum indextype {
	spstar,
	opstar,
	pinstar,
	poutstar,
	ostar,
	};
public static Column getColumn(){
	Column column = new Column();
	
	return column;
}

public static Client getClient(){
	TFramedTransport tf = new TFramedTransport(tr);
	TProtocol tp = new org.apache.thrift.protocol.TBinaryProtocol(tf);
	Cassandra.Client client = new Cassandra.Client(tp);
	return client;
}


}
