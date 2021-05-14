package comparer;

import java.util.Comparator;

import index_elements.Index;

public class IndexComparer implements Comparator<Index> {
	public int compare(Index a, Index b) {
		if(a.getLine_number() == b.getLine_number())
			return a.getStarting_index() > b.getStarting_index()? 1:-1;
		else
			return a.getLine_number() > b.getLine_number() ? 1:-1;
	}
}
