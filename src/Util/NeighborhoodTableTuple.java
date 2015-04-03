package Util;

import java.util.Set;

public class NeighborhoodTableTuple extends IndexTupleAbstract{
	private String centralVertexLable;
	private String FgPrt;
	private Set<String> AdjSet;

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

	public Set<String> getAdjSet() {
		return AdjSet;
	}

	public void setAdjSet(Set<String> set) {
		AdjSet = set;
	}
	
	@Override
	public String toString() {
		StringBuilder tostring = new StringBuilder();
		tostring.append("Central Vertex: " + centralVertexLable + "\n"
				+ "Fingerprint: " + FgPrt + "\n" + "AdjEdges: \n");
		if (AdjSet != null && AdjSet.size() != 0) {
			for (String s : AdjSet) {
				tostring.append(s + "\n");
			}
		}else{
			tostring.append("null\n");
		}
		return tostring.toString();
	}

	@Override
	public boolean init(String line) {
		// TODO Auto-generated method stub
		return false;
	}
}