package Util;

import java.util.HashMap;
import java.util.Iterator;
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
		StringBuilder sb = new StringBuilder();
		sb.append("sp: "+Sp+" DegMap: ");
		Iterator<String> it = DegMap.keySet().iterator();  // Set类型的key值集合，并转换为迭代器
        while(it.hasNext()){                        
            String key = (String) it.next(); 
            sb.append(key+" ").append(DegMap.get(key)+" ");     
        }
		return sb.toString();
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
