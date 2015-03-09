package ffdIndex;
import java.util.StringTokenizer;




public class SpoFingerprint {
	private static final int edgeByteLength = 97;
	private static final int vertexByteLength = 149;
	
	// in and out edge bytes
	private static byte[] inEdgeBytes = new byte[edgeByteLength];
	private static byte[] outEdgeBytes = new byte[edgeByteLength];

	// in and out vertex bytes
	private static byte[] inVertexBytes = new byte[vertexByteLength];
	private static byte[] outVertexBytes = new byte[vertexByteLength];
	
	// The first hash method: BKDRHash method
	private static int BKDRHash(String str, int length) {
		int seed = 131;
		int hash = 0;
		
		for(char ch : str.toCharArray()) {
			hash = (hash * seed + ch) & Integer.MAX_VALUE;
		}
		
		return hash%length;
	}
	
	//The second hash method: APHash
	private static int APHash(String str, int length) {
		int i, size = str.length(), hash = 0;
		
		for(i=0; i<size; i++) {
			if((i & 1) == 0) {
				hash ^= ((hash << 7) ^ (str.charAt(i)) ^ (hash >> 3));
			} else {
				hash ^= (~((hash << 11) ^ (str.charAt(i)) ^ (hash >> 5)));
			}
		}
		
		hash &= Integer.MAX_VALUE;
		
		return hash%length;
	}
	
	//The third hash method: DJBHash method
	private static int DJBHash(String str, int length) {
		int hash = 5381;
		
		for(char ch : str.toCharArray()) {
			hash = (hash + (hash << 5) + ch) & Integer.MAX_VALUE;
		}
		
		return hash%length;
	}
	
	public static StringBuffer getFingerPrint(String variable, String query) {
		int k;
		
		// initiate in and out edge bytes
		for(k=0; k<edgeByteLength; k++) {
			inEdgeBytes[k] = 0;
			outEdgeBytes[k] = 0;
		}
		
		// initiate in and out vertex bytes
		for(k=0; k<vertexByteLength; k++) {
			inVertexBytes[k] = 0;
			outVertexBytes[k] = 0;
		}
		
		StringTokenizer tokenizer = new StringTokenizer(query);
		
		int hashIndex = 0;
		StringBuffer subjectStr;
		StringBuffer predicateStr;
		StringBuffer objectStr;
		
		while(tokenizer.hasMoreTokens()) {
			subjectStr = new StringBuffer(tokenizer.nextToken());
			predicateStr = new StringBuffer(tokenizer.nextToken());
			objectStr = new StringBuffer(tokenizer.nextToken());
			
//			if(!tokenizer.hasMoreTokens()) {
//				objectStr = new StringBuffer(objectStr.substring(0, objectStr.length()-1));
//			}
			

			
				if(!predicateStr.toString().startsWith("?")) {
					
					hashIndex = BKDRHash(predicateStr.toString().trim(), edgeByteLength);
					outEdgeBytes[hashIndex] = 1;
					hashIndex = APHash(predicateStr.toString().trim(), edgeByteLength);
					outEdgeBytes[hashIndex] = 1;
					hashIndex = DJBHash(predicateStr.toString().trim(), edgeByteLength);
					outEdgeBytes[hashIndex] = 1;
				}
				
				if(!objectStr.toString().startsWith("?")) {
					//System.out.println("*"+objectStr.toString()+"*");
					hashIndex = BKDRHash(objectStr.toString().trim(), vertexByteLength);
					outVertexBytes[hashIndex] = 1;
					hashIndex = APHash(objectStr.toString().trim(), vertexByteLength);
					outVertexBytes[hashIndex] = 1;
					hashIndex = DJBHash(objectStr.toString().trim(), vertexByteLength);
					outVertexBytes[hashIndex] = 1;
				}

				
				if(!subjectStr.toString().startsWith("?")) {
					hashIndex = BKDRHash(subjectStr.toString().trim(), vertexByteLength);
					inVertexBytes[hashIndex] = 1;
					hashIndex = APHash(subjectStr.toString().trim(), vertexByteLength);
					inVertexBytes[hashIndex] = 1;
					hashIndex = DJBHash(subjectStr.toString().trim(), vertexByteLength);
					inVertexBytes[hashIndex] = 1;
				}
			

		
		}
		
		StringBuffer sb = new StringBuffer("");
		
		for(byte by : outEdgeBytes) {
			sb.append(by);
		}
		
		for(byte by : outVertexBytes) {
			sb.append(by);
		}
		
		for(byte by : inEdgeBytes) {
			sb.append(by);
		}
		
		for(byte by : inVertexBytes) {
			sb.append(by);
		}		
		
		return sb;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		StringBuffer sb = getFingerPrint("?o", "<http://www.Department20.University15.edu/Course65> <http://www.Department6.University49.edu/#name>  <http://www.Department6.University49.edu/Course65>");
		System.out.println(sb);
	}

}
