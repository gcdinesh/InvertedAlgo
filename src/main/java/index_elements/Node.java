package index_elements;

import java.util.TreeSet;

import comparer.DocumentComparer;

public class Node {
	private String word;
	private TreeSet<Document> document = new TreeSet<Document>(new DocumentComparer());
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	public TreeSet<Document> getDocument() {
		return document;
	}
	public void setDocument(TreeSet<Document> document) {
		this.document = document;
	}
}