package ffdIndex;

import java.io.IOException;

public class Driver {
private static String _keyspace = "ffd";
private static String _spstarColumnfamily = "spstar";
private static String _opstarColumnfamily = "opstar";
private static String _ststarColumnfamily = "ststar";
private static String _pinstarColumnfamily = "pinstar";
private static String _poutstarColumnfamily = "poutstar";
private static String _otstarColumnfamily = "otstar";
private static String _neighborhoodColumnfamily = "nbh";
public static void main(String args[]) {
	
	if (args.length < 3)
    {
        System.out.println("Expecting <input_file> as argument");
        System.out.println("Usage:");
        System.out.println("Construct neigborhood table: <raw_data_path> <0> keyspace");
        System.out.println("Construct sp_star: <1> <raw_data_path> keyspace");
        System.out.println("Construct op_star: <2> <raw_data_path> keyspace");
        System.out.println("Construct s_star: <3> <raw_data_path> keyspace");
        System.out.println("Construct pin_star: <4> <raw_data_path> keyspace");
        System.out.println("Construct pout_star: <5> <raw_data_path> keyspace");
        System.out.println("Construct o_star: <6> <raw_data_path> keyspace");
        System.out.println("query: <7> <query>");
        System.exit(1);
    }
		//neighborhood table
	if(args[1].equals("0")){
		NeighborhoodTableGenerator neighborhoodTableGenerator = new NeighborhoodTableGenerator(args[0], args[2], _neighborhoodColumnfamily);
		try {
			neighborhoodTableGenerator.build();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//spstar
	}else if(args[1].equals("1")){
		DataTransBinarytoHex dataTransBinarytoHex = new DataTransBinarytoHex(args[0], args[2],_spstarColumnfamily);
		if(dataTransBinarytoHex.trans()){
			System.out.println("SP_star data trans successfull!");
		}
		
		//opstar
	}else if(args[1].equals("2")){
		DataTransBinarytoHex dataTransBinarytoHex = new DataTransBinarytoHex(args[0], args[2],_opstarColumnfamily);
		if(dataTransBinarytoHex.trans()){
			System.out.println("OP_star data trans successfull!");
		}
		//ststar
	}else if(args[1].equals("3")){
		DataTransBinarytoHex dataTransBinarytoHex = new DataTransBinarytoHex(args[0], args[2],_ststarColumnfamily);
		if(dataTransBinarytoHex.trans()){
			System.out.println("S_star data trans successfull!");
		}
		//pinstar
	}else if(args[1].equals("4")){
		DataTransBinarytoHex dataTransBinarytoHex = new DataTransBinarytoHex(args[0], args[2],_pinstarColumnfamily);
		if(dataTransBinarytoHex.trans()){
			System.out.println("P_star data trans successfull!");
		}
		//poutstar
	}else if(args[1].equals("5")){
		DataTransBinarytoHex dataTransBinarytoHex = new DataTransBinarytoHex(args[0], args[2],_poutstarColumnfamily);
		if(dataTransBinarytoHex.trans()){
			System.out.println("P_star data trans successfull!");
		}
		//otstar
	}else if(args[1].equals("6")){
		DataTransBinarytoHex dataTransBinarytoHex = new DataTransBinarytoHex(args[0], args[2],_otstarColumnfamily);
		if(dataTransBinarytoHex.trans()){
			System.out.println("O_star data trans successfull!");
		}
		//query
	}else if(args[1].equals("7")){
		
	}else{
		System.exit(1);
	}
	
	
}
}
