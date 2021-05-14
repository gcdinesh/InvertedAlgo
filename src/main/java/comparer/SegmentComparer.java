package comparer;

import java.util.Comparator;

import index_elements.Segment;

public class SegmentComparer implements Comparator<Segment> {

	public int compare(Segment a, Segment b) {
		return a.getThreadNumber() > b.getThreadNumber() ? 1:-1;
	}
	
}
