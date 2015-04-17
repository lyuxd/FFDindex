package Util;

import java.util.HashSet;
import java.util.Set;

public class Star {
	private  String centralVertexLable;
	private  int degreeCount=0;
	private  Set<String> neighborVertex = new HashSet<String>();
	private  StringBuffer surrounding = new StringBuffer();
	
	
	
	public String getCentralVertexLable() {
		return centralVertexLable;
	}
	public void setCentralVertexLable(String centralVertexLable) {
		this.centralVertexLable = centralVertexLable;
	}
	public int getDegreeCount() {
		return degreeCount;
	}
	public void setDegreeCount(int count) {
		this.degreeCount = count;
	}
	public void increaseDegreeCount(){
		this.degreeCount = this.degreeCount+1;
	}
	public Set<String> getNeighborVertex() {
		return neighborVertex;
	}
	public void setNeighborVertex(Set<String> neighborVertex) {
		this.neighborVertex = neighborVertex;
	}
	public void addNeighborVertex(String neighborVertex){
		this.neighborVertex.add(neighborVertex);
	}
	public void enhanceSurrounding(String triple){
		this.surrounding.append(" "+triple);
	}
	public StringBuffer getSurrounding() {
		return surrounding;
	}
	public void setSurrounding(StringBuffer surrounding) {
		this.surrounding = surrounding;
	}
	
	

}
