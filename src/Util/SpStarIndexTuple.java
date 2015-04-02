package Util;

import java.util.HashMap;
import java.util.Map;

public class SpStarIndexTuple extends IndexTupleAbstract{
	private String Sp;
	private Map<String,Integer> DegMap;
	
	public String getSp() {
		return Sp;
	}
	public void setSp(String sp) {
		this.Sp = sp;
	}
	public Map<String, Integer> getDegMap() {
		return DegMap;
	}
	public void setDegMap(Map<String, Integer> degMap) {
		DegMap = degMap;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return Sp;
	}
	@Override
	//line format: sp\tent1<>deg1<>ent2<>deg2...
	public boolean init(String line) {
		String[] elements = line.split("\t");
		if (elements != null && elements.length == 2) {
			String[] entdeg = elements[1].split("<>");
			if(entdeg.length>0 && entdeg.length%2==0){
				Sp = elements[0];
				DegMap = new HashMap<String,Integer>();
				for(int i =0 ; i<entdeg.length;i=i+2){
					DegMap.put(entdeg[i], Integer.valueOf(entdeg[i+1]));
					}
				}
			}
		// TODO Auto-generated method stub
		return true;
	}

}
