package document_indexer;
import java.io.File;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import comparer.SegmentComparer;
import index_elements.Segment;

/*	
 * SplitFile is used to create 'n' threads based on file size and each thread reads 'n' different parts 
 * 	of the file in parallel
*/
public class SplitFile implements Runnable{
	
	private static String filePath;			//path of the file to be indexed
	private static int numberOfThreads;		//number of threads to be created for the file to be read
	private static CountDownLatch latch;	//used for making main thread to wait for it's child thread to get completed
	private int numberOfBytes;				//number of bytes to be read by each thread
	private int offset;						//number of bytes a thread needs to skip before start reading the file
	private int numberOfLines;				//number of lines a thread has read from the file
	private int fileLength;					//number of bytes in the file
	private int extraBytes;					//extra bytes to be read by a thread from it's last read byte until new line is reached
	private File file;						//file object for opening the file to be indexed
	private Thread[] thread;
	
	
	public static TreeSet<Segment> segments = new TreeSet<Segment>(new SegmentComparer());		//collection of Segment arranged in ascending order based on thread number 
	
	public SplitFile(String filePath){
		SplitFile.filePath = filePath;
		offset=0;							//initially no need to skip any bytes from the file
		numberOfBytes = 1024;
		file = new File(SplitFile.filePath);
		fileLength=(int) file.length();
		SplitFile.numberOfThreads = (int) Math.ceil(fileLength/numberOfBytes);
		if(numberOfThreads==0)numberOfThreads++;
		SplitFile.segments.clear();
		thread = new Thread[numberOfThreads];
	}
	
	public SplitFile(int numberOfBytes,int offset){
		this.numberOfBytes = numberOfBytes;
		this.offset = offset;
		numberOfLines=0;
	}
	
	//Creates 'n' threads and starts them with the offset value as the starting position for each thread
	public void readChunksFromFile() {
		try {
			int numberOfThreadsCreated=0;
			
			while(offset<fileLength) {										//if the starting read position of a thread exceeds the file length then stop creation of thread;
				extraBytes=getLineLength(filePath,numberOfBytes,offset);
				
				if(fileLength-offset < numberOfBytes) {					//if number of bytes to be read by the thread exceeds the number of bytes left for reading then set numberOfBytes to remaining bytes to be read only
					numberOfBytes=fileLength-offset;
				}
				
				thread[numberOfThreadsCreated] = new Thread(new SplitFile(numberOfBytes+extraBytes,offset));	//create thread with the number of bytes to be read and the number of bytes to skip before starting to read
				offset+=(extraBytes+numberOfBytes);		//add the number of bytes read by the current thread to the offset so that the next thread skips these bytes
				numberOfThreadsCreated++;
			}
			
			
			latch = new CountDownLatch(numberOfThreadsCreated);		//initializes the latch instances' count value to numberOfThreads
			
			for(int j=0;j<numberOfThreadsCreated;j++) {
				thread[j].start();
			}
			latch.await();		//waiting for the child threads to get finished (waits till the latch's count value becomes zero)
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	//skips offset number of bytes and starts reading specified number of bytes from that point 
	public void run() {
		int numberOfBytesRead=0;
		byte[] byteCharacter = new byte[1];								//To store each single byte read
		String originalString="";										//To concatenate each single byte read from the file
		RandomAccessFile randomAccessFile = null;
		
		try {
				randomAccessFile = new RandomAccessFile(filePath, "r");	
				randomAccessFile.seek(offset);							//Skips the offset number of bytes
				randomAccessFile.read(byteCharacter);					//Reads starting byte of the thread from the file

				while(numberOfBytesRead<numberOfBytes){					//reads the until specified number of bytes is reached 
					numberOfBytesRead++;
					originalString+=new String(byteCharacter);							//concatenate the single character
					if(byteCharacter[0]=='\n') {						//checks whether the character is new line or not 
						numberOfLines++;
					}
					randomAccessFile.read(byteCharacter);				//reads next character from the file
				}
				
				synchronized(SplitFile.class){							//The Segment instance stores the  -> threadNumber, the string obtained and number of lines this thread has read	
					Segment segment = new Segment( getThreadNumber(),originalString,numberOfLines);
					segments.add(segment);
				}
				randomAccessFile.close();
				
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			//decrease the latch's count value by 1
			latch.countDown();
		}
	}
	
	//returns the thread number by extracting it from the ThreadName
	private int getThreadNumber() {
		String threadName=Thread.currentThread().getName();
		int i=7, length=threadName.length(), threadNumber=0;
		while(i<length) {
			threadNumber = threadNumber*10 + (threadName.charAt(i)-48);
			i++;
		}
		return threadNumber;
	}
	
	//segment's line number contains number of lines each thread has processed to replace this with the starting line number this function is used
	public void setSegmentLineNumber() {
		int i=1;
		int[] lineNumberArray = new int[numberOfThreads];		//used to store number of lines processed by each thread and calculate the starting position of the next thread in the file
		Iterator<Segment> iterator = segments.iterator();
		
		if(numberOfThreads == 1) {
			iterator.next().setLineNumber(0);						//if number of thread is one then it's starting line number will be zero in the file
		}
		else {
			lineNumberArray[0]=0;								//starting line number for thread-0(first thread) will be zero
			
			while(i<segments.size()) {
				lineNumberArray[i]=iterator.next().getLineNumber();	//storing the number of lines processed by each thread in the array
				i++;
			}
				
			for(i=1;i<segments.size();i++) {	
				lineNumberArray[i]+=lineNumberArray[i-1];		//calculating the starting line number for thread i and storing it in lineNumberArray(i), note:lineNumberArray(0)=0
			}
		
			i=0;
			iterator = segments.iterator();
			while(iterator.hasNext()) {
				iterator.next().setLineNumber(lineNumberArray[i++]);//replacing the number of lines processed in the segment with starting line number for that segment(or thread)
			}
		}
	}
	
	//To find how many extra bytes are needed from the current thread's ending byte position to end of the line
	public int getLineLength(String filepath,int numberOfBytes,int offset) {
		int extraBytes=0;						// To store the extra bytes needed, initially no bytes needed ,hence it is zero
		char ch;
		RandomAccessFile raf = null;
		
		try {	
			raf = new RandomAccessFile(filepath, "r");
			raf.seek(offset+numberOfBytes);		//offset->previous thread's bytes, length->current thread's bytes	
			
			ch = (char)raf.read();
		
			if(ch==65535) {						//checks whether the current byte is EOF or not
				raf.close();
				return 0;
			}
			else if(ch=='\n') {					//checks whether the current byte is new line or not
				raf.close();		
				return 1;						// if it is new line then we need to add this single byte also, hence return 1
			}
			
			while(ch!='\n' && ch!=65535) {		//check whether new line or EOF
				ch = (char)raf.read();			//read the character if it is not new line or EOF
				extraBytes++;					//increase the extraBytes by one since this character is needed
			}
			if(ch=='\n')
				extraBytes++;					//increase here by 1 since to add the new line character also
			
			raf.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return extraBytes;
	}
}