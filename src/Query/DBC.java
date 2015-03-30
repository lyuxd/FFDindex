package Query;

import Util.Tuple;

public class DBC {
	public Tuple queryExecute(Query query, Index index){
				
		return index.getValueForKey(query.getQuery());
		
	}
}
