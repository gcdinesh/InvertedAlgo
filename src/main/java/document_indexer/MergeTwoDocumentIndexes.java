package document_indexer;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class MergeTwoDocumentIndexes {
	
	private final String GLOBAL_INDEX_FILE_PATH="./Global_Index.json";
	
	public void mergeIndexes(JsonNode rootNode1, String filePath2) {
		try {
			int ch=0;
			byte[] jsonData2 = Files.readAllBytes(Paths.get(filePath2));
			JsonNode Node1 = null,Node2 = null;
			ObjectMapper objectMapper = new ObjectMapper();
			
			//for outputting in a readable json format
			objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
			
			//converting the bytes read into a DOM like structure for easy reading
			JsonNode rootNode2 = objectMapper.readTree(jsonData2);
			
			//for storing the merged index globally
			JsonNode temp = objectMapper.readTree("{\"index\":[]}");
			//global index array
			ArrayNode globalRoot = (ArrayNode) temp.path("index");
			
			
			//rootNode1 and rootNode2 contains array of elements
			Iterator<JsonNode> iterator1 = rootNode1.elements();	//returns the rootNode1 array elements
			Iterator<JsonNode> iterator2 = rootNode2.elements();	//returns the rootNode2 array elements
			
			//always atleast one value will be present in both the documents
			Node1 = iterator1.next();
			Node2 = iterator2.next();
			
			
			while(iterator1.hasNext() && iterator2.hasNext()){
				String a = ((ObjectNode)Node1).path("word").asText();
				String b = ((ObjectNode)Node2).path("word").asText();
				ch=a.compareTo(b);
				if(ch==0) {	// string a == string b
					ArrayNode arrayNode = (ArrayNode) Node1.path("documents");	//returns an array of documents
					ObjectNode objectNode = (ObjectNode) Node2.path("documents").get(0);//Node2 always contains only one document
					arrayNode.add(objectNode);				//adding the document of Node2 to available the documents in Node1
					((ObjectNode)Node1).put("documents",arrayNode);	//replace the older documents array with new updated documents
					globalRoot.add(Node1);					//add this Node to Global Index array
					
					Node1=iterator1.next();
					Node2=iterator2.next();
				}
				else if(ch<0) { //string a < string b
					globalRoot.add(Node1);
					Node1=iterator1.next();
				}
				else {	//string a > string b
					globalRoot.add(Node2);
					Node2=iterator2.next();
				}
			}
			//Note: that Node1 and Node2 value are still not updated to the global index since loop gets terminated
			
			//if both iterator1 and iterator2 reached last elements in both the rootNode1 and rootNode2
			if(!(iterator1.hasNext()) && !(iterator2.hasNext())) {
				globalRoot=lastElements(Node1,Node2,globalRoot);
			}
			//if only iterator2 of rootNode2 reached last Element
			else if(!(iterator2.hasNext())){
				while(iterator1.hasNext()) {
					String a = ((ObjectNode)Node1).path("word").asText();
					String b = ((ObjectNode)Node2).path("word").asText();
					ch=a.compareTo(b);
					if(ch==0) { //check last element of rootNode2 and current element of rootNode1 equal or not
						ArrayNode an = (ArrayNode) Node1.path("documents");
						ObjectNode on = (ObjectNode) Node2.path("documents").get(0);
						an.add(on);
						((ObjectNode)Node1).put("documents",an);
						globalRoot.add(Node1);
						break;					//break since both the elements are merged and only rootNode2 has elements
					}
					else if(ch<0) {//check whether current element of rootNode1 is less than last element of rootNode2
						globalRoot.add(Node1);
						Node1=iterator1.next();
					}
					else {//check whether current element of rootNode1 is greater than last element of rootNode2
						globalRoot.add(Node2);		//first add rootNode2 element to the global index since it is smaller
						globalRoot.add(Node1);		//next add rootNode1 element to the global index
						break;						//break the loop since all elements of rootNode2 added to the global index
													//Note: still rootNode1 has elements
					}
					if(!iterator1.hasNext()) {
						//if both ierator1 of rootNode1 and iterator2 of rootNode2 reached last elements
						globalRoot=lastElements(Node1,Node2,globalRoot);
					}
				}
			}
			//if only iterator1 of rootNode1 reached last Element
			else {
				while(iterator2.hasNext()) {
					String a = ((ObjectNode)Node1).path("word").asText();
					String b = ((ObjectNode)Node2).path("word").asText();
					ch=a.compareTo(b);
					if(ch==0) {	//check last element of rootNode1 and current element of rootNode2 equal or not
						ArrayNode an = (ArrayNode) Node1.path("documents");
						ObjectNode on = (ObjectNode) Node2.path("documents").get(0);
						an.add(on);
						((ObjectNode)Node1).put("documents",an);
						globalRoot.add(Node1);
						break;	//break since both the elements are merged and only rootNode2 has elements
					}
					else if(ch<0) { //check whether last element of rootNode1 is less than current element of rootNode2
						globalRoot.add(Node1);	//then add rootNode1 element
						globalRoot.add(Node2);	//and then add rootNode2 element
						break;					//break the loop since all elements of rootNode1 added to the global index
												//Note: still rootNode2 has elements
					}
					else {	//check whether last element of rootNode1 is greater than current element of rootNode2
						globalRoot.add(Node2);
						Node2=iterator2.next();
					}
					if(!iterator2.hasNext()) {
						//if both ierator1 of rootNode1 and iterator2 of rootNode2 reached last elements
						lastElements(Node1,Node2,globalRoot);
					}
					
				}
			}
			
			//add remaining elements of rootNode1
			while(iterator1.hasNext()) {
				globalRoot.add(iterator1.next());
			}
			//add remaining elements of rootNode2
			while(iterator2.hasNext()) {
				globalRoot.add(iterator2.next());
			}
			
			//increase the number of documents in the global index
			int numberOfDocs=globalRoot.get(globalRoot.size()-1).path("documents").get(0).path("freq").asInt();
			((ObjectNode)(globalRoot.get(globalRoot.size()-1).path("documents").get(0))).put("freq",numberOfDocs+1);
			
			//create a new file Global_Index.json and put the global root values in that
			objectMapper.writeValue(new File(GLOBAL_INDEX_FILE_PATH), globalRoot);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	//function for checking last two elements of the rootNode1,rootNode2 and merging them with the global root 
	public ArrayNode lastElements(JsonNode Node1, JsonNode Node2, ArrayNode globalRoot) {
		String a = ((ObjectNode)Node1).path("word").asText();
		String b = ((ObjectNode)Node2).path("word").asText();
		int ch=a.compareTo(b);
		if(ch==0) {
			ArrayNode an = (ArrayNode) Node1.path("documents");
			ObjectNode on = (ObjectNode) Node2.path("documents").get(0);
			an.add(on);
			((ObjectNode)Node1).put("documents",an);
			globalRoot.add(Node1);
		}
		else if(ch<0) { //a<b
			globalRoot.add(Node1);
			globalRoot.add(Node2);
		}
		else {	//a>b
			globalRoot.add(Node2);
			globalRoot.add(Node1);
		}
		return globalRoot;
	}
}