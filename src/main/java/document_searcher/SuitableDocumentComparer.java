package document_searcher;

import java.util.Comparator;

public class SuitableDocumentComparer implements Comparator<BestDocument> {

	public int compare(BestDocument a, BestDocument b) {
		return a.count > b.count? -1:1;
	}
}
