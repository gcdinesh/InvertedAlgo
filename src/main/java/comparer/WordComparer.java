package comparer;

import java.util.Comparator;

import index_elements.Node;

public class WordComparer implements Comparator<Node> {
	
	public int compare(Node a, Node b) {
		return (a.getWord()).compareTo(b.getWord());
	}
}
