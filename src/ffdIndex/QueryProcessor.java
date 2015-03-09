package ffdIndex;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;


import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;

import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.thrift.Cassandra.AsyncProcessor.system_add_column_family;

import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;
import static org.apache.cassandra.utils.ByteBufferUtil.toInt;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;





public class QueryProcessor {
	private static final ConsistencyLevel CL = ConsistencyLevel.ONE;
	
	
	
	/******************This Line!!!!!!!*/
	private static final String spstar = "spstar";
	/******************/
	
	
	
	
	private static final String opstar = "opstar";
	private static final String ststar = "ststar";
	private static final String pstar = "spstar";
	private static final String otstar = "otstar";
	//private static final String spstar = "spstar";
	private static final String nbh = "nbh";
	//private static final String kspace = "yago";
	private static final String kspace = "ffd";
	private static final TTransport tr = new TSocket("1.2.3.103", 9160);
	private static final String utf8 = "UTF8";
	private static int candidateNumBeforeVerifyFP = 0;
	private static int candidateNumAfterVerifyFP = 0;
	
 public static void main(String args[]) 
		 throws UnsupportedEncodingException, InvalidRequestException,
 UnavailableException, TimedOutException, TException, NotFoundException
 {
	if(args.length != 4 ){
		
		System.out.println("Arguments are need!");
		System.out.println("centervertex degree query sp");
		return;
	}
	
	String centerVar = args[0];
	int queryDegree = new Integer(args[1]);
	String query = args[2];
	//byte[] key = "<http://www.Department9.University9.edu/Lecturer4><#teacherOf>".getBytes();
	byte[] key = args[3].getBytes();
	String queryFingerprint= StringHash.getFingerPrint(centerVar, query).toString();
	
	//System.out.println("1:  "+queryFingerprint);
	TFramedTransport tf = new TFramedTransport(tr);
	TProtocol tp = new org.apache.thrift.protocol.TBinaryProtocol(tf);
	Cassandra.Client client = new Cassandra.Client(tp);
//	Cassandra.Client client=Util.getClient();
	
	long start=0, end=0, resoponsetime=0;
	long accessNBH=0, responseNBH=0, timeNBH=0;
	long accessIndex=0, responseIndex=0, timeIndex=0;
	long startFpMatching=0, endPfMatching=0, timeMatching=0;
//	long timestamp = System.currentTimeMillis() * 1000;
	try {
		tf.open();
	} catch (TTransportException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	try {
		client.set_keyspace(kspace);
	} catch (InvalidRequestException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (TException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	if (true/*args[0].trim().equals(spstar)*/) {
		ColumnPath spColumnPath = new ColumnPath(spstar);
		ColumnPath nbhColumnPath = new ColumnPath(nbh);
		spColumnPath.setColumn("star".getBytes());
		BigInteger queryFPbiginInteger = new BigInteger(queryFingerprint, 2);
		//System.out.println("3:  "+queryFPbiginInteger.toString(2));
		Column column = null;
		
		
		//access index
		accessIndex = System.currentTimeMillis();
		column = client.get(java.nio.ByteBuffer.wrap(key),spColumnPath,CL).getColumn();
		responseIndex = System.currentTimeMillis();
		timeNBH = responseIndex-accessIndex;
		
		
		//String name = new String(column.getName(), utf8);
		String value = new String(column.getValue(), utf8);
		//System.out.println("name: "+name+"    value: "+value+"    resoponsetime: "+resoponsetime);
		
		
		
		
		//access NBH table
		String[] cenandDeg = value.split(" ");
		if(cenandDeg.length%2==0){
			candidateNumBeforeVerifyFP = cenandDeg.length/2;
			startFpMatching = System.currentTimeMillis();
			nbhColumnPath.setColumn("nbh".getBytes());
			for(int i =0; i<cenandDeg.length;i+=2)
			{
				
/*				nbhColumnPath.setColumn("eds".getBytes());
				//start = System.currentTimeMillis();
				column = client.get(java.nio.ByteBuffer.wrap(cenandDeg[i].trim().getBytes()),nbhColumnPath,CL).getColumn();
				//end = System.currentTimeMillis();
				System.out.println(new String(column.getValue(), utf8));
*/
				//get degree. degree of star graph should not be smaller than that of query
//				nbhColumnPath.setColumn("deg".getBytes());
//				accessIndex = System.currentTimeMillis();
//				column = client.get(ByteBuffer.wrap(cenandDeg[i].trim().getBytes()),nbhColumnPath,CL).getColumn();
//				responseIndex = System.currentTimeMillis();
//				timeIndex += (responseIndex-accessIndex);
//				
//				ByteBuffer subgraphDegreeBuffer = ByteBuffer.wrap(column.getValue());
//				int subgraphDegree = toInt(subgraphDegreeBuffer);
				int subgraphDegree = new Integer(cenandDeg[i+1]);
				if (subgraphDegree >= queryDegree) {
					//get fp for each cv 
					nbhColumnPath.setColumn("fp".getBytes());
					accessIndex = System.currentTimeMillis();
					column = client.get(ByteBuffer.wrap(cenandDeg[i].trim().getBytes()),nbhColumnPath,CL).getColumn();
					responseIndex = System.currentTimeMillis();
					timeIndex += (responseIndex-accessIndex);
					//match fp between candidate subgraphs and query.
					ByteBuffer fpbuffer = ByteBuffer.wrap(column.getValue());
					String fpString = bytesToHex(fpbuffer);
					//System.out.println("name: "+new String(column.getName())+"   fpstring:"+fpString);
					BigInteger subgraphFPbigInteger = new BigInteger(fpString,16);
					//System.out.println("2:  "+subgraphFPbigInteger.toString(2));
					
					
					//System.out.println("3:  "+queryFPbiginInteger.toString(2));
					//System.out.println("4:  "+subgraphFPbigInteger.add(queryFPbiginInteger).toString(2));
					if((subgraphFPbigInteger.and(queryFPbiginInteger)).equals(queryFPbiginInteger)){
						//System.out.println("candidate: "+cenandDeg[i].trim());
						candidateNumAfterVerifyFP+=1;
					}else{
						//System.out.println("no");
					}
				}
				
			}//for
			endPfMatching = System.currentTimeMillis();
			timeMatching = endPfMatching - startFpMatching;
			timeNBH = timeMatching-timeIndex;
		}else{
			//error
		}
		
	} else {

	}

	tf.close();
	System.out.println("candidateNumBeforeVerifyFP :  "+candidateNumBeforeVerifyFP);
	System.out.println("candidateNumAfterVerifyFP  :  "+candidateNumAfterVerifyFP);
	System.out.println("timeIndex                  :  "+((float)timeIndex)/1000);
	System.out.println("timeNBH                    :  "+((float)timeNBH)/1000);
	System.out.println("timeMatching(total)        :  "+((float)timeMatching)/1000);
}
 public static ByteBuffer toBytebuffer(String value) throws UnsupportedEncodingException{
	 
	return ByteBuffer.wrap(value.getBytes());
	 
 } 
 
 public static String[] splitIntoCenvDeg(String cenandDeg){
				return cenandDeg.split(" ");
		
		
	}
}

