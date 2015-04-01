package Util;

import java.util.HashSet;

public class NeighborhoodTableTuple {
	private String centralVertexLable;
	private String FgPrt;
	private HashSet<Branch> AdjSet;
	
	
	public String getCentralVertexLable() {
		return centralVertexLable;
	}
	public void setCentralVertexLable(String centralVertexLable) {
		this.centralVertexLable = centralVertexLable;
	}
	public String getFgPrt() {
		return FgPrt;
	}
	public void setFgPrt(String fgPrt) {
		FgPrt = fgPrt;
	}
	public HashSet<Branch> getAdjSet() {
		return AdjSet;
	}
	public void setAdjSet(HashSet<Branch> adjSet) {
		AdjSet = adjSet;
	}
}