package ffdIndex;
/**
 * Disclaimer:
 * This file is an example on how to use the Cassandra SSTableSimpleUnsortedWriter class to create
 * sstables from a csv input file.
 * While this has been tested to work, this program is provided "as is" with no guarantee. Moreover,
 * it's primary aim is toward simplicity rather than completness. In partical, don't use this as an
 * example to parse csv files at home.
 */

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.io.*;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.ByteBufferUtil.hexToBytes;



public class NeighborhoodTableGenerator
{
    private String filename;
    private String keyspace;
    private String columnfamily;
    
    public NeighborhoodTableGenerator(String inputfile, String keyspace, String columnfamily){
    	this.filename = inputfile;
    	this.keyspace = keyspace;
    	this.columnfamily = columnfamily;
    }

    public boolean build () throws IOException
    {
    	@SuppressWarnings("rawtypes")
		IPartitioner partitioner = new Murmur3Partitioner();
        
        //filename = args[0];
        BufferedReader reader = new BufferedReader(new FileReader(filename));

        //String keyspace = "ks_demo";
        File directory = new File(keyspace+"/"+columnfamily);
        if (!directory.exists())
            directory.mkdir();

        		SSTableSimpleUnsortedWriter usersWriter = new SSTableSimpleUnsortedWriter(
                directory,
                partitioner,
                keyspace,
                columnfamily,
                UTF8Type.instance,
                null,
                64);
        String line;
        int lineNumber = 1;
        Entry entry = new Entry();
        // There is no reason not to use the same timestamp for every column in that example.
        long timestamp = System.currentTimeMillis() * 1000;
        while ((line = reader.readLine()) != null)
        {
            if (entry.parse(line, lineNumber))
            {

                ByteBuffer centerVertex = bytes((entry.key));

                usersWriter.newRow(centerVertex);
                usersWriter.addColumn(bytes("eds"), bytes(entry.edges), timestamp);
                usersWriter.addColumn(bytes("deg"), bytes(entry.degree), timestamp);
//                usersWriter.addColumn(bytes("degree"), bytes(entry.degree), timestamp);
                usersWriter.addColumn(bytes("fp"), hexToBytes(entry.fingerprint), timestamp);
//                usersWriter.addColumn(bytes("fp"), bytes(entry.fingerprint), timestamp);
                
            }
            lineNumber++;
        }
        reader.close();
        // Don't forget to close!
        usersWriter.close();
/*
        loginWriter.close();
        
        
*/
        File[] files = directory.listFiles();
		for(File file : files){
			String filename = file.getName();
			if(filename.endsWith("CRC.db")||filename.endsWith("Digest.sha1")||filename.endsWith("Filter.db")
							||filename.endsWith("Statistics.db")||filename.endsWith("TOC.txt")){
				file.delete();
			}
		}
        return true;
    }

    private class Entry
    {
        //UUID key;
        String key;
    	String edges;
        int degree;
        String fingerprint;
        boolean parse(String line, int lineNumber)
        {
            // Ghetto csv parsing
            String[] columns = line.split(" |\t");
            if (columns.length != 4)
            {
                System.out.println(String.format("Invalid input '%s' at line %d of %s", line, lineNumber, filename));
                return false;
            }
            try
            {
                key = columns[0].trim();
                degree = Integer.parseInt(columns[1].trim());
                edges = columns[2].trim();
                fingerprint = (new BigInteger(columns[3].trim(),2)).toString(16);
                if(fingerprint.length()%2==1){
                	fingerprint = "0"+fingerprint;
                }
                return true;
            }
            catch (NumberFormatException e)
            {
                System.out.println(String.format("Invalid number in input '%s' at line %d of %s", line, lineNumber, filename));
                return false;
            }
        }
    }
}
