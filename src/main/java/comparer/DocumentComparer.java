package comparer;

import java.util.Comparator;

import index_elements.Document;

public class DocumentComparer implements Comparator<Document> {
	public int compare(Document a, Document b) {
		return a.getId() > b.getId() ? 1:-1;
	}
}
