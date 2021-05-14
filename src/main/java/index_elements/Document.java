package index_elements;

import java.util.TreeSet;

import comparer.IndexComparer;

public class Document{
	private int id;
	private int frequency;
	private TreeSet<Index> index = new TreeSet<Index>(new IndexComparer());
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getFrequency() {
		return frequency;
	}
	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}
	public TreeSet<Index> getIndex() {
		return index;
	}
	public void setIndex(TreeSet<Index> index) {
		this.index = index;
	}
	
}