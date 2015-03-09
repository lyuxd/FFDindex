package ffdIndex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;





public class DataTransBinarytoHex {
	//private String sp = null;
	//private String edge = null;
	private String keyspace = null;
	private String columnfamily = null;
	//private String tableKey = "sp";
	private String tablevalue = "star";
	private String rawDataPath = null;
	//private String outPutFile = null;
	
	
	/**
	 * Inputfilepath <i>inputfile</i> keyspace <i>keyspace</i> <i>columnfamily</i>.
	 * 
	 * @param inputfile
	 * @param keyspace
	 * @param columnfamily
	 * @return
	 */
	public DataTransBinarytoHex(String inputfile, String keyspace, String columnfamily){
		assert(inputfile!=null);
		assert(keyspace!=null);
		assert(columnfamily!=null);
		
		this.rawDataPath = inputfile;
		this.keyspace = keyspace;
		this.columnfamily = columnfamily;
		//this.tableKey = tablekey;
		
	}
	
	
	public boolean trans(){
		try{
			
			//output directory
			File directory = new File(keyspace+"/"+columnfamily);
	        if (!directory.exists())
	            directory.mkdir();
			
	        @SuppressWarnings("rawtypes")
			IPartitioner partitioner = new Murmur3Partitioner();
	        SSTableSimpleUnsortedWriter usersWriter = new SSTableSimpleUnsortedWriter(
	                directory,
	                partitioner,
	                keyspace,
	                columnfamily,
	                UTF8Type.instance,
	                null,
	                128);
			//read file content
			
			StringBuffer sb = new StringBuffer("");
			FileReader fr = new FileReader(rawDataPath);
			BufferedReader br = new BufferedReader(fr);
			long timestamp = System.currentTimeMillis() * 1000;
			String line = null;
			long lineNumber = 1;
			while(null!=(line = br.readLine())){
				sb.setLength(0);
				String[] key_value = line.split("___!!!___");
				
				// length !=2, a demaged line.
				if (key_value.length != 2){
					System.out.println(String.format("Invalid input '%s' at line %d of %s", line, lineNumber, rawDataPath));
					continue;
				}
/*
				//the second elements is "[centerVertex degree fingerprint[ centerVertex degree fingerprint...]]"
				String[] cv_deg_fp = key_value[1].split(" ");
				
				// cv_deg_fp length must be 3*n
				if (cv_deg_fp.length%2!=0){
					System.out.println(String.format("Invalid input '%s' at line %d of %s", line, lineNumber, rawDataPath));
					continue;
				}
				for(int i=0; i<cv_deg_fp.length;i++){
					if((i+1)%3==0){
					BigInteger bigInteger = new BigInteger(cv_deg_fp[i],2);
					cv_deg_fp[i]=bigInteger.toString(16);	
					}//if
					sb.append(cv_deg_fp[i]+" ");
				}//for
				
				
				bytes(sb.toString());
				System.out.println(key_value[0]);
				System.out.println(sb);
*/				
				usersWriter.newRow(bytes(key_value[0].trim()));
//				usersWriter.addColumn(bytes(tablevalue), bytes(sb.toString()), timestamp);
				usersWriter.addColumn(bytes(tablevalue), bytes(key_value[1].trim()), timestamp);
				
				
				
				lineNumber++;
			}//while read line
			usersWriter.close();
			br.close();
			fr.close();

			
			File[] files = directory.listFiles();
			for(File file : files){
				String filename = file.getName();
				if(filename.endsWith("CRC.db")||filename.endsWith("Digest.sha1")||filename.endsWith("Filter.db")
								||filename.endsWith("Statistics.db")||filename.endsWith("TOC.txt")){
					file.delete();
				}
			}
			
		}catch(FileNotFoundException e){
			e.printStackTrace();
			return false;
		}catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		
		
		
		return true;
		
	}
}
