package com.gsc.LIA;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author gsc
 *
 */


public class IndexAndSearch {

	/**
	 * Directory for the Index to Store/search.
	 */
	private Directory indexDirectory;
	
	/**
	 * analyzer for query/doc
	 */
	private Analyzer analyzer;

	
	/**
	 * build index for search
	 * @param srcPath
	 * 	Path to the source txt file to index
	 * @param destPath
	 * 	Path to the storage of generated index file
	 * 
	 * 	(/tmp folder recommended because many Linux systems mount /tmp as tmpfs in RAM)
	 * @throws Exception
	 */
	public void buildIndex(String srcPath,String destPath)  throws Exception	{
		analyzer = new StandardAnalyzer(Version.LUCENE_40);
		File touchDir=new File(destPath);//make sure the directory exists
		touchDir.mkdir();
		indexDirectory = FSDirectory.open(touchDir);
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_40, analyzer);
		IndexWriter iwriter = new IndexWriter(indexDirectory, config);

		Gson GSON_BUILDER = (new GsonBuilder()).disableHtmlEscaping().create();
		String jsonInput = srcPath;
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(jsonInput), "UTF-8"));
		String line;
	
		while((line = br.readLine()) != null) {
			//解析
			Weibo weibo = GSON_BUILDER.fromJson(line, Weibo.class);
			Document document=new Document();
			//using Field to allow for more flexible search functions
			document.add(new Field("uid",weibo.uid,StringField.TYPE_STORED));
			document.add(new Field("content",weibo.content,TextField.TYPE_STORED));//TextField for tokenizing
			document.add(new Field("commentnum",weibo.commentnum,StringField.TYPE_STORED));
			document.add(new Field("time",weibo.time,StringField.TYPE_STORED));
			document.add(new Field("rtnum",weibo.rtnum,StringField.TYPE_STORED));

			iwriter.addDocument(document);

		}
		iwriter.close();
		br.close();
	}

	/**
	 * @param queryString
	 * 	The query String to be searched (Operators such as AND allowed and Fields like uid:1000000 can be used to restrict query
	 * @param indexPath
	 * 	Path to index file
	 * @param num
	 * 	Number of results to show
	 * @return
	 * 	whether found documents
	 * @throws Exception
	 */
	public boolean  searchIndex(String queryString, String indexPath,int num)throws Exception{
		boolean found= false;
		indexDirectory = FSDirectory.open(new File(indexPath));
		analyzer = new StandardAnalyzer(Version.LUCENE_40);
		DirectoryReader directoryReader=DirectoryReader.open(indexDirectory);
		IndexSearcher indexSearcher=new IndexSearcher(directoryReader);

		QueryParser parser=new QueryParser(Version.LUCENE_40,"content",analyzer);
		Query query=parser.parse(queryString);
		long start= System.currentTimeMillis();
		ScoreDoc[] hitsDocs=indexSearcher.search(query, null, 1000).scoreDocs;
		long end=System.currentTimeMillis();
		if (hitsDocs.length<1) {
			System.out.println("Found No Document relevant!");
			return found;
		}
		found=true;
		//System.out.println("#Hits:"+hitsDocs.length);
		System.out.println("Found "+(hitsDocs.length)+" document(s) (in "+(end-start)+" ms) that matched query'"+queryString+"':");
		int showing=1;
		if (num==-1) {					//for showing all results
			showing=hitsDocs.length;
			System.out.println("Showing all:");
		}
		else {
			showing=num;
			System.out.println("Showing "+ num+" results:");
		}
		for(int i=0;i<showing;i++){
			Document hitDocument=indexSearcher.doc(hitsDocs[i].doc);
			System.out.println("Doc #"+(i+1)+":"+hitDocument.get("content"));
			SimpleDateFormat sdf=new SimpleDateFormat("MMM dd,yyyy HH:mm");//translate time
			Date resultDate=new Date(Long.parseLong(hitDocument.get("time"))*1000);
			System.out.println("uid:"+hitDocument.get("uid")+"\trtnum:"+hitDocument.get("rtnum")+"\tcommentnum:"+hitDocument.get("commentnum")+"\tPost time:"+sdf.format(resultDate));
			System.out.println();
		}
		directoryReader.close();

		return found;
	}

	public static void main(String[] args)throws Exception  {

		IndexAndSearch is=new IndexAndSearch();
		//is.buildIndex();
		//		is.indexDirectory = FSDirectory.open(new File("/home/gsc/tmp/index"));
		//		is. analyzer = new StandardAnalyzer(Version.LUCENE_40);
		//		is.searchIndex();
		//	
		CommandLineParser lineParser=new PosixParser();	//Apache CommonCLI
		Options options=new Options();
		//options.addOption("i", "index",false,"build index data from the -s <path> and store it to -d <path>");
		options.addOption("s", "src", true, "src path to the txt file for index");
		options.addOption("i", "index", true, "path of the index");
		options.addOption("q", "query", true, "query to search");
		options.addOption("n","num",true,"number of results to return");

		CommandLine cmdLine=lineParser.parse(options, args);
		HelpFormatter formatter=new HelpFormatter();//for usage printing
		
		if (cmdLine.hasOption("i")==true) {					
			if (cmdLine.hasOption("s")) {
				String srcPath=cmdLine.getOptionValue("s");
				String destPath=cmdLine.getOptionValue("i");
				is.buildIndex(srcPath, destPath);
				System.out.println("Successfully built Index at "+destPath+" from"+srcPath);
			}else if (cmdLine.hasOption("q")) {
				//String queryString=options.getOption("q").getValue();
				//String indexPath=options.getOption("i").getValue();
				String queryString=cmdLine.getOptionValue("q");
				String indexPath=cmdLine.getOptionValue("i");
				if (cmdLine.hasOption("n")) {
					int num=Integer.parseInt(cmdLine.getOptionValue("n"));
					is.searchIndex(queryString, indexPath, num);
				}
				else {
					is.searchIndex(queryString, indexPath, -1);
				}
			}else {
				formatter.printHelp("IndexAndSearch", options);
			}
		} 
		else {
			formatter.printHelp("IndexAndSearch", options);
		}
	}
}
