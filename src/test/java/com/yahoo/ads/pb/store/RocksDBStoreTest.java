package com.yahoo.ads.pb.store;


import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;

public class RocksDBStoreTest {

	private static final String baseDir = "/tmp/store_0";
	private static final String baseDir1 = "/tmp/store_1";
	

	
	@BeforeClass
	public static void setUp() throws Exception {
		// prepare the directory for data store
		FileUtils.forceMkdir(new File(baseDir ));
		FileUtils.forceMkdir(new File(baseDir1));
	}

	@AfterClass
	public static void tearDown() throws Exception {
		// remove the data directory
		FileUtils.forceDelete(new File(baseDir));
		FileUtils.forceDelete(new File(baseDir1));
	}

	@Test
	public void testRocksDBStore() throws Exception {
		RocksDBStore store = new RocksDBStore(
				baseDir, 
				1, 
				8, 
				2000000, 
				1000L * 1024 * 1024);
		store.open();
	
		
		store.store(12345L, 0, "Hello".getBytes());
		store.store(12346L, 0, "Hello1".getBytes());


		// close and reopen the store
		store.close();
		
		store.open(0);
		
		
		assertEquals("Should get value back.", "Hello", new String(store.get(12345L,0)));
		
		//assertEquals(2, store.count());
		
		store.store(12345L, 0, "World".getBytes());

		assertEquals("Should get new value back.", "World", new String(store.get(12345L,0)));
		
		assertEquals(true, store.delete(12345L));

		store.store(0L,0, null);
		assertEquals(null, store.get(0,0));

		store.close();
	}
	
}

