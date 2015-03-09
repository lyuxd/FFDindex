package ffdIndex;


public class testFpPerformance {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String spoString = "<http://www.Department0.University0.edu/AssistantProfessor0> <http://www.Department0.University0.edu/#teacherOf> <http://www.Department0.University0.edu/GraduateCourse40>";
		System.out.println(StringHash.getFingerPrint("?o", spoString).toString());
		long starTime  = System.currentTimeMillis();
		for(int i = 32962704;i>=0;i--){
			SpoFingerprint.getFingerPrint("?o", spoString);
		}
		long endTime = System.currentTimeMillis();
			System.out.println(((double)endTime-starTime)/1000);
		
	}

}
