package document_indexer;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import comparer.WordComparer;
import index_elements.Document;
import index_elements.Index;
import index_elements.Node;
import index_elements.Segment;


/*
 * This class contains methods to create individual threads for each segment and generate the Nodes(Index) for each the word 
 * obtained from the file.
 * At last the Nodes obtained are merged together and written to the index.json file as JSON.
 */
public class GenerateNodes implements Runnable{

	 
	private static TreeSet<Node> documentNodes = new TreeSet<Node>(new WordComparer());	//Stores all generated Nodes in ascending order
	private static ArrayList<Node> allSegmentNodes = new ArrayList<Node>();				//Stores all the Nodes generated for each segment   (i.e.) segment-1 Nodes +segment-2 Nodes+...
	private static CountDownLatch latch;												//Used to make main thread to wait for child threads	
	private static int id;																//Current Document id
	private Segment segment;															//Segment contains threadNumber, string from the file, starting line number
	private Thread[] thread;
	
	public GenerateNodes(int id) {
		GenerateNodes.id=id;
		documentNodes.clear();
		allSegmentNodes.clear();
		thread = new Thread[SplitFile.segments.size()];
		latch = new CountDownLatch(SplitFile.segments.size());
	}
	
	public GenerateNodes(Segment tempSegment) {
		this.segment = tempSegment;
	}

	public void createThreadForNodeGeneration() {
		try {
			int numberOfThreads=0;
			
			for(Segment tempSegment:SplitFile.segments) {								//the segments collection contains all the string read from the file (segment is in the SplitFile class)
				thread[numberOfThreads] = new Thread(new GenerateNodes(tempSegment));
				numberOfThreads++;
			}
			for(int j=0;j<numberOfThreads;j++) {
				thread[j].start();
			}
			latch.await();																//waits for child threads to complete it's execution
			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void run() {
		TreeSet<Node> segmentNodes;
		
		segmentNodes = generateNodesForSegment(segment.getLineNumber(), segment.getString());		//returns all the Nodes generated from that particular Segment
		synchronized(GenerateNodes.class) {												//since node is static and threads may concurrently update the collection, here the class lock is used for updating
			allSegmentNodes.addAll(segmentNodes);
		}
		latch.countDown();																//if the thread job is completed latch decreases the value by 1
	}
	
	//extracts words from the segment's string and creates Nodes for them and return the fully Merged Nodes of this Segment
	public TreeSet<Node> generateNodesForSegment(int lineNumber, String string) {
		
		TreeSet<Node> segmentNodes = new TreeSet<Node>(new WordComparer());
		
		string=string.replace("\r", "");
		String[] lineSplit=string.split("\n");
		for(String line:lineSplit) {
			int lineLength=line.length();
			int numberOfBytesRead=0;
			int actualCount=0;											//Stores the Column number of the word in original file (i.e) if "tab" is present then increases by 4
			
			while(numberOfBytesRead<lineLength) {
				String exactWord="";
				int startingIndex = actualCount;
				for(int i=numberOfBytesRead;i<lineLength;i++,numberOfBytesRead++,actualCount++) {
					char ch=line.charAt(i);
					
					if(ch>=65 && ch<=90){
						exactWord+=(char)(line.charAt(i)+32);			//if the character is capital letter then change it to small letter
					}
					else if(ch>=97 && ch<=122) {
						exactWord+=line.charAt(i);
					}
					else {												//if any special character comes then break the loop
						if(ch=='\t')
							actualCount+=4;
						else 
							actualCount++;
						
						numberOfBytesRead++;
						break;
					}
				}
				
				if(exactWord!="") {												//this condition fails when numbers or special characters occurs continuosly
					Node node=createNode(exactWord,startingIndex,lineNumber);	//creates the Node for only one word
					segmentNodes=mergeNodes(node,segmentNodes);					//merge this Node with already existing Nodes
				}
			}
			lineNumber++;
		}
		return segmentNodes;
	}
	
	//Creates Node and stores the word, line number, starting Index in that Node and returns it
	private Node createNode(String exactWord, int startingIndex,int lineNumber) {
		Node node = new Node();
		node.setWord(exactWord);
		Document document = new Document();
		document.setId(id);
		document.setFrequency(1);
		Index index = new Index();
		index.setLine_number(lineNumber);
		index.setStarting_index(startingIndex);
		document.getIndex().add(index);
		node.getDocument().add(document);
		return node;
	}
	
	//"GeneratedNodes.nodes" contains each segment's nodes separately, 
	// hence merging all the segment nodes into a single collection(wordIndex)
	public void generateFullIndex() {
		for(Node node:allSegmentNodes) {
			documentNodes=mergeNodes(node, documentNodes);
		}
	}
	
	//used to merge a single Node with the global index
	public TreeSet<Node> mergeNodes(Node node,TreeSet<Node> documentNodes){
		
		Node copyNode=null;
		
		for(Node tempNode:documentNodes) {
			if(tempNode.getWord().equals(node.getWord())) {
					Document tempDocument=tempNode.getDocument().first();
					node.getDocument().first().getIndex().addAll(tempDocument.getIndex());		//copies all the indexes from the tempDocument to the node's document
					node.getDocument().first().setFrequency(node.getDocument().first().getFrequency()+tempDocument.getFrequency());//updates the frequency in the node's document
					copyNode=tempNode;
					break;
			}
		}
		
		if(copyNode!=null) {
			documentNodes.remove(copyNode);										//remove the old node from the document Nodes
			documentNodes.add(node);											//add the new updated node in the document Nodes
		}
		else {
			documentNodes.add(node);
		}
		
		return documentNodes;
	}
	
	//extracts each Node from the documentNodes and writes them to the Index.json file in JSON format
	public void writeASJsonToFile(String filePath) {
		System.out.println("word:"+documentNodes.size());
		try {
		FileWriter fw = new FileWriter(new File(filePath));
		int wordLength = documentNodes.size();
		int tempWordLength=0;
		fw.write("[");
		for(Node tempDocumentNode:documentNodes) {
			fw.write("\r\n{\r\n\t\"word\":\""+tempDocumentNode.getWord()+"\",");
			fw.write("\r\n\t\"documents\":[");
			int documentLength = tempDocumentNode.getDocument().size();
			int tempDocumentLength=0;
			for(Document doc:tempDocumentNode.getDocument()) {
				fw.write("\r\n\t\t{\r\n\t\t\t\"id\":"+doc.getId()+",");
				fw.write("\r\n\t\t\t\"freq\":"+doc.getFrequency()+",");
				fw.write("\r\n\t\t\t\"indexes\":[");
				int indexLength = doc.getIndex().size();
				int tempIndexLength=0;
				for(Index ind:doc.getIndex()) {
					fw.write("\r\n\t\t\t\t{\r\n\t\t\t\t\t\"lineNumber\":"+ind.getLine_number()+",");
					fw.write("\r\n\t\t\t\t\t\"startingIndex\":"+ind.getStarting_index()+"\r\n\t\t\t\t}");
					if(tempIndexLength!=indexLength-1)
						fw.write(",");
					tempIndexLength++;
				}
				fw.write("\r\n\t\t\t]\r\n\t\t}");
				if(tempDocumentLength!=documentLength-1)
					fw.write(",");
				tempDocumentLength++;
				fw.write("\r\n\t]");
			}
			fw.write("\r\n}");
			if(tempWordLength!=wordLength-1)
				fw.write(",");
			tempWordLength++;
		}
		fw.write("]");
		fw.close();
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
}