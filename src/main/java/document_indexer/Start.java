package document_indexer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Start {
	
	private final String GLOBAL_INDEX_FILE_PATH="./Global_Index.json";
	private final String CURRENT_DOCUMENT_INDEX_FILE_PATH="./temp/index.json";
	
	public static void main(String[] args) {
		Start start = new Start();
		start.SplitIndexMerge("./text1.txt");
	}
	
	private void SplitIndexMerge(String filePath) {
		
		try {
			File file = new File(GLOBAL_INDEX_FILE_PATH);
			if(!file.exists()) {
				createGlobalIndexFile(file);
			}
			byte[] index = Files.readAllBytes(Paths.get(GLOBAL_INDEX_FILE_PATH));
			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode globalIndexNodes = objectMapper.readTree(index);
			int id=globalIndexNodes.get(globalIndexNodes.size()-1).path("documents").get(0).path("freq").asInt();
			id++;
			
			File document = new File(filePath);
			if(document.length()==0) {
				System.err.print("The File is either Empty or not Found.");
				System.exit(0);
			}
			
			/*Splitting a given text document into 'n' chunks and reading them individually with 'n' threads.
			 *create instance for SplitFile class and pass the filepath and name(id) of the document to the constructor*/
			SplitFile splitFile = new SplitFile(filePath);
			
			//readChunksFromFile method used for reading the file in parallel using 'n' threads and storing each thread's read bytes in Segment
			splitFile.readChunksFromFile();
			
			//each Segment has separate "number of lines" read by each thread, so add previous thread's line numbers
			//to get the current thread's starting line number.
			splitFile.setSegmentLineNumber();
			
			/*Generates the Node(Index) structure for each word in the Segment and merges those Indexes together 
			 *and writes them to the file as JSON.*/
			GenerateNodes generateNodes = new GenerateNodes(id);
			generateNodes.createThreadForNodeGeneration();
			generateNodes.generateFullIndex();
			generateNodes.writeASJsonToFile(CURRENT_DOCUMENT_INDEX_FILE_PATH);
			
			//merging two document indexes Global_Index.json and Index.json 
			//Finally the merged indexes are available in Global_Index.json
			MergeTwoDocumentIndexes mergeTwoDocumentIndexes = new MergeTwoDocumentIndexes();
			mergeTwoDocumentIndexes.mergeIndexes(globalIndexNodes,CURRENT_DOCUMENT_INDEX_FILE_PATH);
			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void createGlobalIndexFile(File file) {
		try {
			FileWriter fileWriter = new FileWriter(file);
			fileWriter.write("[\n\t{\n\t\t\"word\" : \"~\",");
			fileWriter.write("\n\t\t\"documents\" : [");
			fileWriter.write("\n\t\t\t{");
			fileWriter.write("\n\t\t\t\t\"id\" : 0,");
			fileWriter.write("\n\t\t\t\t\"freq\" : 0,");
			fileWriter.write("\n\t\t\t\t\"indexes\" : [");
			fileWriter.write("\n\t\t\t\t\t{");
			fileWriter.write("\n\t\t\t\t\t\t\"line_number\" : 0,");
			fileWriter.write("\n\t\t\t\t\t\t\"starting_position\" : 0");
			fileWriter.write("\n\t\t\t\t\t}");
			fileWriter.write("\n\t\t\t\t]");
			fileWriter.write("\n\t\t\t}");
			fileWriter.write("\n\t\t]");
			fileWriter.write("\n\t}");
			fileWriter.write("\n]");
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}