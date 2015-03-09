package ffdIndex;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;


import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;

import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;


import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;





public class SimpleReader {
	private static final ConsistencyLevel CL = ConsistencyLevel.ONE;
	private static final String spstar = "spstar";
	private static final String nbh = "nbh";
	private static final TTransport tr = new TSocket("1.2.3.103", 9160);
	private static final String utf8 = "UTF8";
	
 public static void main(String args[]) 
		 throws UnsupportedEncodingException, InvalidRequestException,
 UnavailableException, TimedOutException, TException, NotFoundException
 {
	

	TFramedTransport tf = new TFramedTransport(tr);
	TProtocol tp = new org.apache.thrift.protocol.TBinaryProtocol(tf);
	Cassandra.Client client = new Cassandra.Client(tp);
	long start=0, end=0, resoponsetime=0;
//	long timestamp = System.currentTimeMillis() * 1000;
	try {
		tf.open();
	} catch (TTransportException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	try {
		client.set_keyspace("ks_demo");
	} catch (InvalidRequestException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (TException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	
	ColumnPath columnPath = new ColumnPath(spstar);
	columnPath.setColumn("star".getBytes());
	byte[] key = "<http://www.Department0.University0.edu/AssistantProfessor1><#teacherOf>".getBytes();
	Column column = null;
	try {
		start = System.currentTimeMillis();
		column = client.get(java.nio.ByteBuffer.wrap(key),columnPath,CL).getColumn();
		end = System.currentTimeMillis();
		
	} catch (InvalidRequestException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (NotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (UnavailableException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (TimedOutException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (TException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	
	
	
	//print the value
//	if(null!=column){

//	try {
//		String name = new String(column.getName(), utf8);
//		String value = new String(column.getValue(), utf8);
//		
//		//fingerprint of query graph Q*
//		String raw ="000010100000010"+"0"/* 16th bit is 1 in fp(G*) */+"1100000010111010110000000001000000000010100000000100000100010100000001000101100000101000000101110000101010001001"+
//				"00001010000"+"0"/* 112th bit is 0 in fp(G*) */+"01011100000010111010110000000001000000000010100000000100000100010100000001000101100000101000000101110000101010001001"+
//				"00001010000001011100000010111010110000000001000000000010100000000100000100010100000001000101100000101000000101110000101010001001"+
//				"00001010000001011100000010111010110000000001000000000010100000000100000100010100000001000101100000101000000101110000101010001001"+
//				"000010100000010111000000101110101100000000010000000000101000000001000001000101000000010001011000001010000001011100001010";
//		
//		String rawx ="000010100000010"+"0"/* 16th bit is 1 in fp(G*) */+"1100000010111010110000000001000000000010100000000100000100010100000001000101100000101000000101110000101010001001"+
//				"00001010000"+"0"/* 112th bit is 0 in fp(G*) */+"01011100000010111010110010000001010000000010100000000100000100010100000001000101100000101000000101110000101010001001"+
//				"00001010010001011100000010111010110000000001000000000010100001000100000100010100100001000101100000101000000101110010101010001001"+
//				"00001010010001011100000010111010110001000001001000100010100010000100000100010100000001000101100000101000000100000000101010001001"+
//				"000010100100010111000000101110101101000000010000100000101000010001000001000101000100010001011000000010100001011100101010";
//		//fingerprint of data graph G*
//		String raw2 ="00001010000001011100000010111010110000000001000000000010100000000100000100010100000001000101100000101000000101110000101010001001"+
//				"00001010000001011100000010111010110000000001000000000010100000000100000100010100000001000101100000101000000101110000101010001001"+
//				"00001010000001011100000010111010110000000001000000000010100000000100000100010100000001000101100000101000000101110000101010001001"+
//				"00001010000001011100000010111010110000000001000000000010100000000100000100010100000001000101100000101000000101110000101010001001"+
//				"000010100000010111000000101110101100000000010000000000101000000001000001000101000000010001011000001010000001011100001010";
//		String raw3= "1010";
//		int a = 0x80000000;
//		int c= 0x7a046d26;
//		
//		BigInteger bigInteger = new BigInteger(raw, 2);
//		BigInteger bigInteger2 = new BigInteger(raw2, 2);
//		long start = System.currentTimeMillis();
//		String hexraw = bigInteger.toString(16);
//		String hexraw2 = bigInteger2.toString(16);
//		
//		
//		BigInteger bI = new BigInteger(hexraw, 16);
//		BigInteger bI2 = new BigInteger(hexraw2,16);
//		long end = System.currentTimeMillis();
//		System.out.println(((float)(end-start))/1000);
//		//if  fp(Q*)&fp(G*) = fp(Q*)  then Q* covers G*.
//		//if((bigInteger.and(bigInteger2)).equals(bigInteger)){
//		if((bI.and(bI2)).equals(bI)){
//			System.out.println("yes");
//		}else{
//			System.out.println("no");
//		}
//		//String hexraw = bigInteger.toString(16);
//		
//		//System.out.println(bigInteger.toString(16));
//		System.out.println(c);
//		int b = a | 0x7FFFFFFF;
//		b = b>>1;
//		b=b<<1;
//		System.out.println(b);
//		
//		
//		
//		System.out.println("name: "+name+"    value: "+value);
//	} catch (UnsupportedEncodingException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
//		}//if
	
//	//insert into users
//	ColumnParent columnParent = new ColumnParent(spstar);
//	Column insertColumn = new Column(toBytebuffer("firstname"));
//	insertColumn.setValue(toBytebuffer("lxd2"));
//	insertColumn.setTimestamp(timestamp);
//	client.insert(toBytebuffer("123"), columnParent, insertColumn, CL);
	
	
	/*//insert into userss
	ColumnParent columnParent2 = new ColumnParent(columnFamily2);
	Column insertColumn2 = new Column(toBytebuffer("firstname"));
	insertColumn.setValue(toBytebuffer("lxd"));
	insertColumn.setTimestamp(timestamp);
	client.insert(toBytebuffer("123"), columnParent2, insertColumn2, CL);
	*/
	
	resoponsetime = end-start;
	String name = new String(column.getName(), utf8);
	String value = new String(column.getValue(), utf8);
	System.out.println("name: "+name+"    value: "+value+"    resoponsetime: "+resoponsetime);
	
	tf.close();
}
 public static ByteBuffer toBytebuffer(String value) throws UnsupportedEncodingException{
	 
	return ByteBuffer.wrap(value.getBytes());
	 
 } 
}

