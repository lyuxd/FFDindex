package Query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import SigGen.StringHash;
import Util.Star;

public class Query {
	private String query;
	private Map<String, Integer> sortedMapOnDegree;
	private List<String> queryOrderList = new ArrayList<String>();
	Map<String, Star> subQueryMap = new HashMap<String, Star>();

	public Query() {

	}

	public Query(String query) {
		this.query = query;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public boolean prepare() {
		if (query == null || query.equals("")) {
			return false;
		}

		// Map<String, String> subQueries = new TreeMap<String,String>();
		Map<String, Integer> centralDegree = new HashMap<String, Integer>();

		String[] elements = query.split("\\s+|\\.\\s+");
		String[] triples = query.split("\\.\\s+");
		if (elements.length % 3 != 0) {

			return false;
		}
		for (int i = 0; i < elements.length; i = i + 3) {
			centralDegree.put(
					elements[i],
					centralDegree.get(elements[i]) == null ? 1 : centralDegree
							.get(elements[i]) + 1);
			centralDegree.put(elements[i + 2],
					centralDegree.get(elements[i + 2]) == null ? 1
							: centralDegree.get(elements[i + 2]) + 1);

			if (i % 3 == 0) {
				Star star = subQueryMap.get(elements[i]);
				if (star == null) {
					star = new Star();
				}
				star.setCentralVertexLable(elements[i]);
				star.increaseDegreeCount();
				star.addNeighborVertex(elements[i + 2]);
				star.enhanceSurrounding(triples[i / 3]);
				star.setFingerprint(StringHash.getFingerPrint(
						star.getCentralVertexLable(),
						star.getSurrounding().toString()).toString());
				subQueryMap.put(elements[i], star);

				star = subQueryMap.get(elements[i + 2]);
				if (star == null) {
					star = new Star();
				}
				star.setCentralVertexLable(elements[i + 2]);
				star.increaseDegreeCount();
				star.addNeighborVertex(elements[i]);
				star.enhanceSurrounding(triples[i / 3]);
				star.setFingerprint(StringHash.getFingerPrint(
						star.getCentralVertexLable(),
						star.getSurrounding().toString()).toString());
				subQueryMap.put(elements[i + 2], star);

			}

		}
		sortedMapOnDegree = sortMap(centralDegree);
		// printMap(sortedMapOnDegree);
		boolean degreeIsZero = false;
		Map<String, Integer> tempMap = new LinkedHashMap<String, Integer>(
				sortedMapOnDegree);
		// printMap(tempMap);
		while (!degreeIsZero) {
			Iterator<Map.Entry<String, Integer>> it = tempMap.entrySet()
					.iterator();
			if (it.hasNext()) {
				Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) it
						.next();
				String key = entry.getKey();
				// System.out.println("key: "+key);

				int value = entry.getValue();
				// System.out.println("degree: "+value);
				if (value == 0) {
					degreeIsZero = true;
					break;
				}
				for (String neighbor : subQueryMap.get(key).getNeighborVertex()) {
					Integer degree = (Integer) tempMap.get(neighbor);
					if (degree != null) {
						degree = degree - 1;
						tempMap.put(neighbor, degree);
					}
				}
				queryOrderList.add(key);
				// printMap(tempMap);
				tempMap.remove(key);
				// printMap(tempMap);
				tempMap = sortMap(tempMap);

			}
		}

		return true;
	}

	public static Map<String, Integer> sortMap(Map<String, Integer> oldMap) {
		ArrayList<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(
				oldMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {

			@Override
			public int compare(Entry<java.lang.String, Integer> arg0,
					Entry<java.lang.String, Integer> arg1) {
				return arg1.getValue() - arg0.getValue();
			}
		});
		Map<String, Integer> newMap = new LinkedHashMap<String, Integer>();
		for (int i = 0; i < list.size(); i++) {
			newMap.put(list.get(i).getKey(), list.get(i).getValue());
		}
		return newMap;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("===================mapStart==================\n");
		Iterator<Entry<String, Integer>> it = sortedMapOnDegree.entrySet()
				.iterator();
		while (it.hasNext()) {
			Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) it
					.next();
			sb.append(entry.getKey() + ":" + entry.getValue() + "\n");
		}
		sb.append("===================mapEnd==================\n");
		return sb.toString();
	}

	public void printStringIntegerMap(Map<String, Integer> map) {
		StringBuffer sb = new StringBuffer();
		sb.append("===================mapStart==================\n");
		Iterator<Map.Entry<String, Integer>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) it
					.next();
			sb.append(entry.getKey() + ":" + entry.getValue() + "\n");
		}
		sb.append("===================mapEnd==================\n");
		System.out.print(sb.toString());
	}

	public void printSubQueryMap() {
		Iterator<String> it = this.subQueryMap.keySet().iterator();
		for (; it.hasNext();) {
			Star star = subQueryMap.get(it.next());
			System.out.println(star.getCentralVertexLable() + ": <"
					+ (star.getDegreeCount()) + ","
					+ (star.getSurrounding().toString()) + ">"
					+ (star.getFingerprint()));
		}
	}

	public void printQueryOrderList() {
		System.out.println("QueryOrderList size: " + queryOrderList.size());
		System.out.println("QueryOrderList content: ");
		for (Iterator<String> it = queryOrderList.iterator(); it.hasNext();) {
			System.out.println((String) it.next());
		}
		System.out.println("-------------------------");

	}

	public void execute(Query query) {
		
	}
}
