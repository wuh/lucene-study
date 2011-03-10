package test.index;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 * 测试IndexWriter的各种参数设置
 * 
 *
 * @author wuhao
 */
public class TestIndexWriter extends TestCase {

	/**
	 * 索引跟目录
	 */
	private String indexDir = "index";

	/**
	 * 获取索引跟目录，每一个测试都使用自己的目录建立索引。比如测试IndexWriter的MaxFieldLength，那么在index目录下
	 * 会有一个test_indexWriter_MaxFieldLength的目录存放测试索引。
	 * @param testName
	 * @return
	 * @throws IOException
	 */
	private Directory getDir(String testName) throws IOException {
		return FSDirectory.open(new File(new File(indexDir), "test_" + testName));
	}

	private TopDocs search(Directory dir, String fieldName, String keyWord) throws Exception {
		IndexReader reader = IndexReader.open(dir);
		Searcher searcher = new IndexSearcher(reader);
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);
		QueryParser parser = new QueryParser(Version.LUCENE_30, fieldName, analyzer);
		Query query = parser.parse(keyWord);
		TopDocs td = searcher.search(query, Integer.MAX_VALUE);
		reader.close();
		return td;
	}

	/**
	 * 增加Document，commitNum表示提交次数，每一次的提交都会增加一个包含content字段的Document，content字段的内容
	 * 为"i am a doctor" + commitNum。
	 * @param dir
	 * @param analyzer
	 * @param create
	 * @param policy
	 * @param maxFieldLength
	 * @param commitNum
	 * @throws Exception
	 */
	private void addDocument(Directory dir, Analyzer analyzer, boolean create, IndexDeletionPolicy policy,
			MaxFieldLength maxFieldLength, int commitNum) throws Exception {
		if (maxFieldLength == null)
			maxFieldLength = MaxFieldLength.UNLIMITED;
		IndexWriter writer = new IndexWriter(dir, analyzer, create, policy, maxFieldLength);
		for (int i = 0; i < commitNum; i++) {
			Document doc1 = new Document();
			doc1.add(new Field("content", "i am a doctor " + (i + 1), Store.NO, Index.ANALYZED));
			writer.addDocument(doc1);
			writer.commit();
		}
		writer.close();
	}

	/**
	 * 测试IndexWriter的MaxFieldLength属性
	 * <p>
	 * 该属性用于指定对Field进行分词时的最大项数
	 * <p>
	 * 该函数针对字符串  i am a doctor 进行分析：
	 * 当MaxFieldLength为1时，只会对第一个词 i 进行索引，那么搜索am 的时候就不会有结果；
	 * 当修改MaxFieldLength为2时，就会索引到am，那么搜索am就会有结果。
	 * @throws Exception 
	 */
	public void testIndexWriter_MaxFieldLength() throws Exception {
		Directory dir = getDir("IndexWriter_MaxFieldLength");
		//MaxFieldLength为1
		addDocument(dir, new StandardAnalyzer(Version.LUCENE_30), true, null, new MaxFieldLength(1), 1);
		TopDocs td1 = search(dir, "content", "am");
		assertEquals(td1.totalHits, 0);

		//MaxFieldLength为最大值
		addDocument(dir, new StandardAnalyzer(Version.LUCENE_30), true, null, MaxFieldLength.UNLIMITED, 1);
		TopDocs td2 = search(dir, "content", "am");
		assertEquals(td2.totalHits, 1);
	}

	/**
	 * 测试构造函数的create参数
	 * <P>当create=false时，是append操作。当create=true时，是overwrite操作。这个操作只有在commit后才会发生。
	 * @throws Exception
	 */
	public void testIndexWriter_Create() throws Exception {
		Directory dir = getDir("IndexWriter_Create");
		//创建一个新的IndexWriter，写入一个Document
		IndexWriter writer1 = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_30), true,
				MaxFieldLength.UNLIMITED);
		Document doc = new Document();
		doc.add(new Field("content", "i am a doctor", Store.NO, Index.ANALYZED));
		writer1.addDocument(doc);
		writer1.close();

		//创建一个新的IndexWriter，且create=true。这是创建了IndexWriter后，索引不会发生改变，只有在commit后，
		//create为true才起作用。
		IndexWriter writer2 = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_30), true,
				MaxFieldLength.UNLIMITED);
		//此处索引还是第一次的索引，因此可以搜索到
		TopDocs td1 = search(dir, "content", "doctor");
		assertEquals(td1.totalHits, 1);
		writer2.close();
		//此处第二次已经提交，索引被覆盖，就无法搜索到了
		TopDocs td2 = search(dir, "content", "doctor");
		assertEquals(td2.totalHits, 0);
	}

	/**
	 * 测试commit方法
	 * <p>
	 * 只有在commit之后，才能够搜索到索引的内容。如果没有commit，然后虚拟机关闭，那么先前修改的内容是无效的。
	 * @throws Exception
	 */
	public void testIndexWriter_Commit() throws Exception {
		Directory dir = getDir("IndexWriter_Commit");
		//创建一个新的IndexWriter，写入一个Document
		IndexWriter writer1 = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_30), true,
				MaxFieldLength.UNLIMITED);
		Document doc = new Document();
		doc.add(new Field("content", "i am a doctor", Store.NO, Index.ANALYZED));
		writer1.addDocument(doc);
		writer1.close();
		//close即commit，可以搜索到内容
		TopDocs td = search(dir, "content", "doctor");
		assertEquals(td.totalHits, 1);

		IndexWriter writer2 = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_30), true,
				MaxFieldLength.UNLIMITED);
		//先删除掉索引。因为上面增加过document
		writer2.deleteAll();
		writer2.commit();

		Document doc2 = new Document();
		doc2.add(new Field("content", "i am a doctor", Store.NO, Index.ANALYZED));
		writer2.addDocument(doc2);
		//在commit之前搜索，无法搜索到内容
		TopDocs td2 = search(dir, "content", "doctor");
		assertEquals(td2.totalHits, 0);
		writer2.close();
	}

	/**
	 * 测试CommitPoint
	 * <p>
	 * 经测试，每一次commit都会产生一个CommitPoint，对应一个segments_7，如当2次commit后，segments_n会+2。
	 * segments_n保存了提交点的元数据。
	 * <p>
	 * TODO 该CommitPoint有在应用中有何意义？
	 * @throws Exception
	 */
	public void testIndexWriter_CommitPoint() throws Exception {
		Directory dir = getDir("IndexWriter_CommitPoint");
		//采用自定义的KeepLastTwoCommitsDeletionPolicy，保存最近的2个提交点
		addDocument(dir, new StandardAnalyzer(Version.LUCENE_30), true, new KeepLastTwoCommitsDeletionPolicy(), null, 5);
		assertEquals(IndexReader.listCommits(dir).size(), 2);

		//数组从0到最后是最新的commitpoint到最老的commitpoint
		IndexCommit[] ics = (IndexCommit[]) IndexReader.listCommits(dir).toArray(new IndexCommit[2]);
		//打开最近的提交点，最近一次提交的内容为i am a doctor 5，因此搜索5能够搜索到Document
		IndexReader reader1 = IndexReader.open(ics[0], false);
		Searcher searcher1 = new IndexSearcher(reader1);
		Analyzer analyzer1 = new StandardAnalyzer(Version.LUCENE_30);
		QueryParser parser1 = new QueryParser(Version.LUCENE_30, "content", analyzer1);
		Query query1 = parser1.parse("5");
		TopDocs td1 = searcher1.search(query1, Integer.MAX_VALUE);
		assertEquals(td1.totalHits, 1);

		//打开最近提交点的前一个调点，该提交点是第四次提交，提交内容为i am a doctor 4，因此搜索5无法搜索到Document
		IndexReader reader2 = IndexReader.open(ics[1], false);
		Searcher searcher2 = new IndexSearcher(reader2);
		Analyzer analyzer2 = new StandardAnalyzer(Version.LUCENE_30);
		QueryParser parser2 = new QueryParser(Version.LUCENE_30, "content", analyzer2);
		Query query2 = parser2.parse("5");
		TopDocs td2 = searcher2.search(query2, Integer.MAX_VALUE);
		assertEquals(td2.totalHits, 0);
	}

	/**
	 * 测试IndexDeletionPolicy
	 * <p>
	 * IndexDeletionPolicy是CommitPoint的删除策略，CommitPoint表示每次提交的提交点，测试见{@link #testIndexWriter_CommitPoint()}。
	 * 系统默认的策略为保存最近一次的提交点。
	 * TODO 什么情况下才需要改变该策略？
	 * <p>
	 * @throws Exception
	 */
	public void testIndexWriter_IndexDeletionPolicy() throws Exception {
		Directory dir = getDir("IndexWriter_IndexDeletionPolicy");
		//采用默认的KeepOnlyLastCommitDeletionPolicy，只保存最近的提交点
		addDocument(dir, new StandardAnalyzer(Version.LUCENE_30), true, null, null, 5);
		assertEquals(IndexReader.listCommits(dir).size(), 1);

		//采用自定义的KeepLastTwoCommitsDeletionPolicy，保存最近的2个提交点
		addDocument(dir, new StandardAnalyzer(Version.LUCENE_30), true, new KeepLastTwoCommitsDeletionPolicy(), null, 5);
		assertEquals(IndexReader.listCommits(dir).size(), 2);
	}

	/**
	 * 测试numRamDoc，该方法返回当前内存中的文档数
	 * @throws Exception
	 */
	public void testIndexWriter_NumRamDoc() throws Exception {
		Directory dir = getDir("IndexWriter_NumRamDoc");
		IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_30), true,
				MaxFieldLength.UNLIMITED);
		Document doc = new Document();
		doc.add(new Field("content", "i am a doctor", Store.NO, Index.ANALYZED));
		writer.addDocument(doc);
		assertEquals(writer.numRamDocs(), 1);
		writer.commit();
		assertEquals(writer.numRamDocs(), 0);
		writer.close();
	}

	/**
	 * 测试MaxBufferedDocs
	 * MaxBufferedDeleteTerms、RAMBufferSizeMB和该参数相似。
	 * 不同之处在于，MaxBufferedDocs、MaxBufferedDeleteTerms默认为disabled的，而RAMBufferSizeMB默认为16MB。
	 * <p>
	 * MaxBufferedDocs表示最大的缓存文档数，换句话说，就是当缓存中的文档数到底了设置的这个值之前，不会写入索引中。
	 * @throws Exception
	 */
	public void testIndexWriter_MaxBufferedDocs() throws Exception {
		Directory dir = getDir("IndexWriter_MaxBufferedDocs");
		IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_30), true,
				MaxFieldLength.UNLIMITED);
		//先删除掉所有索引
		writer.deleteAll();
		writer.commit();
		int len = 5;
		writer.setMaxBufferedDocs(len);
		//这里add到index的Document数量比设置的最大缓存数量少1，由于没有到底最大缓存数量，因此不会被写入到索引文件中。
		for (int i = 0; i < len + 1; i++) {
			Document doc = new Document();
			doc.add(new Field("content", "i am a doctor " + i + 1, Store.NO, Index.ANALYZED));
			writer.addDocument(doc);
		}
		//增加的Document数量比最大缓存数量大1，因此前面的Document已经被写入到文件中
		assertEquals(writer.numRamDocs(), 1);
		//numDocs也包含了在缓存中的Document
		assertEquals(writer.numDocs(), 6);
		//没有提交，那么搜索不到
		TopDocs td = search(dir, "content", "doctor");
		assertEquals(td.totalHits, 0);
		writer.close();
	}

	/**
	 * 测试InfoStream
	 * <p>
	 * setInfoStream(PrintStream ps)可以传入一个输出流，IndexWriter向该流中写入该IndexWriter执行的信息。
	 * message(String s)可以加入用户的信息。
	 * @throws Exception
	 */
	public void testIndexWriter_InfoStream() throws Exception {
		Directory dir = getDir("IndexWriter_InfoStream");
		IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_30), true,
				MaxFieldLength.UNLIMITED);
		writer.setInfoStream(System.out);
		writer.deleteAll();
		writer.commit();
		writer.message("user message");
		writer.close();
	}

	/**
	 * 测试optimize
	 * <p>
	 * optimize会合并索引文件，根据观察，每次提交产生一个cfs文件，而optimize的过程就是生成各个操作的索引文件（.fdt,.fnm等），
	 * 再将该cfs的内容分散到各个文件中。optimize是有消耗的，频繁的commit和optimize将带来巨大的开销。
	 * @throws Exception
	 */
	public void testIndexWriter_Optimize() throws Exception {
		Directory dir = getDir("IndexWriter_Optimize");
		IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_30), true, null,
				MaxFieldLength.UNLIMITED);
		for (int i = 0; i < 20; i++) {
			Document doc1 = new Document();
			doc1.add(new Field("content", "i am a doctor " + (i + 1), Store.NO, Index.ANALYZED));
			writer.addDocument(doc1);
			writer.commit();
		}
		writer.optimize();
		writer.close();
	}

	/**
	 * 测试mergeFactor
	 * <P>
	 * mergeFactor是合并因子，该参数控制到达多少个cfs文件后，进行optimize操作。
	 * @throws Exception
	 */
	public void testIndexWriter_MaxMergeFactor() throws Exception {
		Directory dir = getDir("IndexWriter_MaxMergeFactor");
		IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_30), true,
				MaxFieldLength.UNLIMITED);
		writer.deleteAll();
		writer.commit();
		writer.setMergeFactor(4);
		for (int i = 0; i < 20; i++) {
			Document doc = new Document();
			doc.add(new Field("content", "i am a doctor " + i + 1, Store.NO, Index.ANALYZED));
			writer.addDocument(doc);
			writer.commit();
		}
		writer.close();
	}

	/**
	 * 测试maxMergeDocs
	 * <p>
	 * 每次的提交会产生一个段文件，该文件包含了该次提交的所有索引信息。该段文件能够被合并的前提条件是该段文件的文档数
	 * <=maxMergeDocs。如果超出maxMergeDocs就不会被合并。因此maxMergeDocs一般设置为很大。
	 * @throws Exception
	 */
	public void testIndexWriter_MaxMergeDocs() throws Exception {
		Directory dir = getDir("IndexWriter_MaxMergeDocs");
		IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_30), true,
				MaxFieldLength.UNLIMITED);
		writer.deleteAll();
		writer.commit();
		int maxmergeDocs = 21;
		//这里设置最大的合并文档数为21
		writer.setMaxMergeDocs(maxmergeDocs);
		//因为默认的合并因子是10，这里提交10次，那么索引会被合并
		for (int j = 0; j < 10; j++) {
			//这里只增加maxmergeDocs-1个文档，因为如果>=maxmergeDocs那么会造成该索引文件不会被合并
			for (int i = 0; i < maxmergeDocs - 1; i++) {
				Document doc = new Document();
				doc.add(new Field("content", "i am a doctor " + i + 1, Store.NO, Index.ANALYZED));
				writer.addDocument(doc);
			}
			writer.commit();
		}
		writer.close();
	}

}
