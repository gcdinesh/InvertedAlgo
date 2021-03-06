package document_searcher;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.TreeSet;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FindSuitableDocumentUsingBinarySearch{

	public static void main(String[] args) {
		try {
			FindSuitableDocumentUsingBinarySearch find = new FindSuitableDocumentUsingBinarySearch();
			String searchString = "private data";										//query string for searching in the gloabl index
			ArrayList<String> words = find.getWords(searchString);					//get individual words from the searchString
			
			//Collection of more suitable documents in descending order based on their count value
			TreeSet<BestDocument> suitableDocuments = new TreeSet<BestDocument>(new SuitableDocumentComparer());
			
			byte[] byteIndex = Files.readAllBytes(Paths.get("./Global_Index.json"));
			ObjectMapper objectMapper = new ObjectMapper();							//creating ObjectMapper instance for converting the bytes into DOM like structure
			JsonNode globalIndex = objectMapper.readTree(byteIndex);
			
			//get the number of documents currently uploaded in the global index
			int numberOfDocs=globalIndex.get(globalIndex.size()-1).path("documents").get(0).path("freq").asInt();
			
			System.out.println("Number of Documents available for search:   "+numberOfDocs);
			
			int[] docs = new int[numberOfDocs+1];									//docs[i] = represents the ith document and its value represents the number of intersection
			for(int i=0;i<docs.length;i++)docs[i]=0;								//initially zero intersections
			
			//if searchString words and global index words match each other then copy and store them in matchedIndexObjects
			ArrayList<JsonNode> matchedIndexObjects = new ArrayList<JsonNode>();
			
			int i = 0, j = 0;
			
			//go through all the words present in the global index and if matches the searchString words then store them in 
			//matchedIndexObjects
			for(String word:words) {
				int low=0, high=globalIndex.size()-1;
				int mid = 0;
				while(low<=high) {
					mid=(low+high)/2;
					if((globalIndex.get(mid).path("word").asText()).compareTo(word) == 0) {
						j=0;
						matchedIndexObjects.add(globalIndex.get(mid));										//add the object to the matchedIndexObjects
						
						while(j<globalIndex.get(mid).path("documents").size()) {							//To increment the document count value which has this matched word
							int id = globalIndex.get(mid).path("documents").get(j).path("id").asInt();		//get the id of each document in which this word is present
							docs[id]++;																		//increment the number of intersections
							j++;
						}
						break;
					}
					else if((globalIndex.get(mid).path("word").asText()).compareTo(word)<0){
						low=mid+1;
					}
					else {
						high=mid-1;
					}
				}
			}
			
			//To find the searchString words which are not present in the global Index
			find.checkWordNotPresentInGlobalIndex(matchedIndexObjects,words);
			
			//document id starts from one 
			for(int k=1;k<docs.length;k++) {
				//create new BestDocument and update it's id and number of intersections
				BestDocument temp = new BestDocument();
				temp.Id=k;
				temp.count=docs[k];
				
				//add this BestDocument to the collections
				suitableDocuments.add(temp);
			}
			
			
			if(matchedIndexObjects.size()==0) {
				System.out.println("NO MATCHED DOCUMENT FOUND");
			}
			else {
				for(BestDocument temp:suitableDocuments) {
					System.out.println("\n\n**********************");
					//to collect the missing words in this document
					ArrayList<String> missingWords = new ArrayList<String>();
					for(i=0;i<matchedIndexObjects.size();i++) {
						int flag=0;
						for(j=0;j<matchedIndexObjects.get(i).path("documents").size();j++) {
							if(matchedIndexObjects.get(i).path("documents").get(j).path("id").asInt() == temp.Id) {
								System.out.println(matchedIndexObjects.get(i).path("word")+":"+matchedIndexObjects.get(i).path("documents").get(j));
								flag=1;
								break;//since only once the id will be matched break here
							}
						}
						if(flag==0) {
							missingWords.add(matchedIndexObjects.get(i).path("word").asText());
						}
					}
					if(missingWords.size() > 0 && missingWords.size()!=matchedIndexObjects.size()) {
						System.out.println("Missing Words:");
						for(String temps:missingWords) {
							System.out.println(temps);
						}
						
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	//used to check the words not available in the global index
	private void checkWordNotPresentInGlobalIndex(ArrayList<JsonNode> json, ArrayList<String> words) {
		ArrayList<String> presentWords = new ArrayList<String>();
		for(JsonNode temp:json) {
			String str=temp.path("word").asText();
			presentWords.add(str);
		}
		words.removeAll(presentWords);
		System.out.println("\nWords not in global index:\t"+words.size());
		for(String temp:words) {
			System.out.println(temp);
		}
		System.out.print("\n");
	}
	
	//remove extract the words separately from the given string
	public ArrayList<String> getWords(String searchString) {
		ArrayList<String> words = new ArrayList<String>();
		int lineLength=searchString.length();
		int count=0;
		while(count<lineLength) {
			String exactWord="";
			for(int i=count;i<lineLength;i++,count++) {
				char ch=searchString.charAt(i);
				if(ch>=65 && ch<=90){
					exactWord+=(char)(searchString.charAt(i)+32);
				}
				else if(ch>=97 && ch<=122) {
					exactWord+=searchString.charAt(i);
				}
				else {
					count++;
					break;
				}
			}
			if(exactWord!="") {
				words.add(exactWord);
			}
		}
		return words;
	}
}