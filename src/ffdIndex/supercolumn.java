package ffdIndex;
/**
 * Disclaimer:
 * This file is an example on how to use the Cassandra SSTableSimpleUnsortedWriter class to create
 * sstables from a csv input file.
 * While this has been tested to work, this program is provided "as is" with no guarantee. Moreover,
 * it's primary aim is toward simplicity rather than completness. In partical, don't use this as an
 * example to parse csv files at home.
 */
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.io.*;
//import java.util.UUID;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
//import org.apache.cassandra.dht.OrderPreservingPartitioner;
//import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;


import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
//import static org.apache.cassandra.utils.UUIDGen.decompose;

public class supercolumn
{
    static String filename;

    public static void main(String[] args) throws IOException
    {
    	@SuppressWarnings("rawtypes")
		IPartitioner partitioner = new Murmur3Partitioner();
        if (args.length == 0)
        {
            System.out.println("Expecting <csv_file> as argument");
            System.exit(1);
        }
        filename = args[0];
        @SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader(new FileReader(filename));

        String keyspace = "ks_demo";
        File directory = new File(keyspace);
        if (!directory.exists())
            directory.mkdir();

        		SSTableSimpleUnsortedWriter usersWriter = new SSTableSimpleUnsortedWriter(
                directory,
                partitioner,
                keyspace,
                "users",
                AsciiType.instance,
                AsciiType.instance,
                64);
/*        SSTableSimpleUnsortedWriter loginWriter = new SSTableSimpleUnsortedWriter(
                directory,
                partitioner,
                keyspace,
                "logins",
                AsciiType.instance,
                null,
                64);
*/
        String line;
        int lineNumber = 1;
        CsvEntry entry = new CsvEntry();
        // There is no reason not to use the same timestamp for every column in that example.
        long timestamp = System.currentTimeMillis() * 1000;
        Map<String, String> map = new HashMap<String, String>();
        map.put("1", "a trick");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(map);
        
        byte[] bytesOutput = byteArrayOutputStream.toByteArray();
        
        byteArrayOutputStream.close();
        objectOutputStream.close();
        ByteBuffer supercolumn = ByteBuffer.wrap(bytesOutput);
        while ((line = reader.readLine()) != null)
        {
            if (entry.parse(line, lineNumber))
            {
            	//System.out.println("########"+entry.key+"########");
                //ByteBuffer uuid = ByteBuffer.wrap(decompose(entry.key));
                ByteBuffer uuid = bytes((entry.key));
                //String uuid = entry.key;
                
            	System.out.println("uuid:  "+ uuid.toString());
            	System.out.println("firstname:  "+ entry.firstname);
            	System.out.println("age:  "+ entry.age);
            	System.out.println("email:  "+ entry.email);
            	System.out.println("lastname:  "+ entry.lastname);
            	System.out.println("password:  "+ entry.password);
                usersWriter.newRow(uuid);
                usersWriter.addColumn(bytes("firstname"), bytes(entry.firstname), timestamp);
                usersWriter.addColumn(bytes("age"), bytes(entry.age), timestamp);
                usersWriter.addColumn(bytes("email"), bytes(entry.email), timestamp);
                usersWriter.addColumn(bytes("lastname"), bytes(entry.lastname), timestamp);
                usersWriter.addColumn(bytes("password"), bytes(entry.password), timestamp);
                usersWriter.newSuperColumn(supercolumn);

            }
            lineNumber++;
        }
        // Don't forget to close!
        usersWriter.close();
/*
        loginWriter.close();
*/
        System.exit(0);
    }

    static class CsvEntry
    {
        //UUID key;
        String key;
    	String firstname;
        String lastname;
        String password;
        int age;
        String email;

        boolean parse(String line, int lineNumber)
        {
            // Ghetto csv parsing
            String[] columns = line.split(",");
            if (columns.length != 6)
            //if (columns.length != 4)
            {
                System.out.println(String.format("Invalid input '%s' at line %d of %s", line, lineNumber, filename));
                return false;
            }
            try
            {
                //key = UUID.fromString(columns[0].trim());
                key = columns[0].trim();
            	//System.out.println(key+"*********************************************");
                firstname = columns[1].trim();
                age = Integer.parseInt(columns[4].trim());
                email = columns[5].trim();
                lastname = columns[2].trim();
                password = columns[3].trim();
/*
                lastname = columns[2].trim();
                password = columns[3].trim();
                age = Integer.parseInt(columns[4].trim());
                email = columns[5].trim();
*/
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
