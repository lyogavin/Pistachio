/*
 * Copyright 2014 Yahoo! Inc. Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or
 * agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.ads.pb.store;



import com.yahoo.ads.pb.util.Convert;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.rocksdb.*;

import com.google.common.primitives.Longs;
//import com.yahoo.ads.pb.platform.profile.ProfileUtil;

public class RocksDBStore {
	private static Logger logger = LoggerFactory.getLogger(RocksDBStore.class);

	private static final int writeBufferSize = 5000;
	
	private final String baseDir;
	private final int numStores;   //0 means use helix to control partition num
	private final int recordSizeAlignment;
	private final int totalRecords;
	private final long mappedMemorySize;
	private final boolean isReadOnly;
	private final Store[] stores;

	public RocksDBStore(
			String baseDir,
			int numStores,
			int recordSizeAlignment,
			int totalRecords,
			long mappedMemorySize) {
		
		this(baseDir, numStores, recordSizeAlignment, totalRecords, mappedMemorySize, false);
	}
	
	public RocksDBStore(
			String baseDir,
			int numStores,
			int recordSizeAlignment,
			int totalRecords,
			long mappedMemorySize,
			boolean isReadOnly) {

		this.baseDir = baseDir;
		this.recordSizeAlignment = recordSizeAlignment;
		this.totalRecords = totalRecords;
		this.mappedMemorySize = mappedMemorySize;
		this.isReadOnly = isReadOnly;
		if (numStores == 0) {
			this.numStores = 0;
			stores = new Store[256];
		} else {
			this.numStores = numStores;
			stores = new Store[numStores];
			for (int i = 0; i < numStores; i++) {
				stores[i] = new Store(i);
			}
		}
	}

	/**
	 * Open the data base
	 */
	public void open() throws Exception {
		for (Store store : stores) {
			store.open();
		}
	}
	
	/**
	 * Open one data base
	 */
	public void open(int partitionId) throws Exception {
		stores[partitionId] = new Store(partitionId);
		stores[partitionId].open();
	}

	/**
	 * Close all database.
	 * @throws RocksDBException 
	 */
	public void close() {
		for (Store store : stores) {
			if(store != null)
				store.close();
		}
	}
	
	/**
	 * Get database index based on input key.
	 * 
	 * @param key
	 * @return
	 */
	private int getDbIndex(long key) {
		if(numStores == 0){
			return (int) (key % 256 >= 0 ? key % 256 : 256+key % 256);
		}else{
			return (int) (key / 1000L % numStores);
		}	
	}

	/**
	 * Store a record.
	 * 
	 * @param key
	 * @param value
	 */
	public void forceFlush(int partitionId) throws Exception {
		if(stores[partitionId] != null)
			stores[partitionId].forceFlush(true);
		else
			throw new Exception("partition "+partitionId+" not exist, can't flush");
	}
	
	/**
	 * Store a record.
	 * 
	 * @param key
	 * @param value
	 */
	public void store(long key, long partition, byte[] value) throws Exception {
		int dbIndex = (int)partition;
        /*
		int dbIndex = getDbIndex(key);
        */
		if(stores[dbIndex] != null)
			stores[dbIndex].store(key, value);
		else
			throw new Exception("partition "+dbIndex+" not exist, can't store");
	}
	
	public void storeOffset( long value, int partitionId) throws Exception {
		if(stores[partitionId] != null)
			stores[partitionId].storeOffset( value);
		else
			throw new Exception("partition "+partitionId+" not exist, can't store offset");
	}
	
	public long getOffset(int partitionId) throws Exception{
		if(stores[partitionId] != null)
			return stores[partitionId].getOffset();
		else
			return 0;
	}

	public boolean isPartitionActive(int partitionId){
		if(stores[partitionId] != null){
			return true;
		}else{
			return false;
		}
	}
	
	/**
	 * Get a record.
	 * 
	 * @param key
	 * @return
	 * @throws RocksDBException 
	 */
	public byte[] get(long key, int partitionId) throws RocksDBException {
		int dbIndex = partitionId; //getDbIndex(key);
		if(stores[dbIndex] != null)
			return stores[dbIndex].get(key);
		else
			return null;
	}

	/**
	 * Remove a record.
	 * 
	 * @param key
	 * @return
	 * @throws RocksDBException 
	 */
	public boolean delete(long key) throws RocksDBException {
		int dbIndex = getDbIndex(key);
		if(stores[dbIndex] != null){
		    return stores[dbIndex].delete(key);	
		} 
		else 
			return false;
	}

	/**
	 * Get total number of records.
	 * 
	 * @return
	 */
	public long count() {
		long count = 0;

		for (Store store : stores) {
			if (store != null) {
				count += store.count();
			}
		}
		return count;
	}

//	/**
//	 * Go through all the records with the visitor.
//	 * 
//	 * @param visitor
//	 */
//	public void iterate(Visitor visitor) {
//		
//	}
//	
//	/**
//	 * Go through all the records with the visitor.
//	 * 
//	 * @param visitor
//	 */
//	public void iterateWithReadLock(Visitor visitor) {
//		
//	}
	
//	/**
//	 * Go through all the records in the single store with visistor
//	 * 
//	 * @param visitor
//	 * @param index
//	 *
//	 */
//	public void iteratorSingleStore(Visitor visitor, int index) {
//
//	}

	private class Store {
		private final int dbIndex;
     	private RocksDB db;

		private volatile ConcurrentHashMap<Long, byte[]> prevMap;
		private volatile ConcurrentHashMap<Long, byte[]> currentMap;
		private volatile ConcurrentHashMap<byte[], byte[]> offsetMap;
		private volatile boolean isClosing;
		private volatile boolean forceFlush;
		private Thread profileWriter;
        private AtomicInteger flushCounter;
    	private final static int PER_OFFSET_FLUSH = 100;
		private long offset  = -1;
		private static final String offsetKey  = "offset_storage_RD";

		private java.util.Random rand = new java.util.Random();
		private static final int maxQueuingCount = 200000;
    	private volatile int queuingSize = 0;
    	
    	private AtomicLong storeCounter = new AtomicLong(0);
    	

		public Store(int dbIndex) { 
			
			RocksDB.loadLibrary();
			
			this.dbIndex = dbIndex;


			currentMap = new ConcurrentHashMap<Long, byte[]>(2 * writeBufferSize);
			offsetMap = new ConcurrentHashMap<byte[], byte[]>(); 
			flushCounter = new AtomicInteger(0);
			forceFlush = false;	
		
		}

		public void forceFlush(boolean forceFlush) {
			this.forceFlush = forceFlush;
			while(forceFlush){
				try {
	                Thread.sleep(1000L);
                } catch (InterruptedException e) {
	                // TODO Auto-generated catch block
	                e.printStackTrace();
                }
			}
		}

		/**
		 * Open the data base
		 */
		public void open() throws Exception {
		    RocksDB.loadLibrary();
		
			Options options = new Options().setCreateIfMissing(true);
			try {
			  db = RocksDB.open(options, "/Users/sophiech/git/Pistachio");
			  storeCounter.set(Convert.bytesToLong(db.get("storeCounter".getBytes())));
			  //assert(false);
			} catch (RocksDBException e) {
			  System.out.format("caught the expceted exception -- %s\n", e);
			  assert(db == null);
			}
			  
			  
			profileWriter = new Thread(new Runnable() {

				@Override
				public void run() {
					while (!isClosing) {
						try {
							flush();
						} catch (RocksDBException e1) {
							e1.printStackTrace();
						}

						try {
							Thread.sleep(100L);
						} catch (InterruptedException e) { 
							e.printStackTrace();
						}
					}

					try {
						flush();
					} catch (RocksDBException e) {
						e.printStackTrace();
					}
				}
			});			  
			  
			profileWriter.setName("ProfileWriter_" + dbIndex);
			profileWriter.setDaemon(true);
			profileWriter.start();
			
			
		}

		/**
		 * Close all database.
		 * @throws RocksDBException 
		 */
		public void close() {
			try {
			    db.put("storeCounter".getBytes(), Convert.longToBytes(storeCounter.get())) ;
			}
			catch (RocksDBException e) { }
			isClosing = true;
			try {
				profileWriter.join();
			}
			catch (InterruptedException e) { }
			if (db != null) {
			      db.close();
			      db.dispose();
			}
			
		}
		
	

		/**
		 * Store a record.
		 * 
		 * @param key
		 * @param value
		 */
		public void store(long key, byte[] value) throws Exception {
			if (ArrayUtils.isEmpty(value)) {
				return;
			}

			if (queuingSize > maxQueuingCount || rand.nextInt(100) == 0) {

				while ((queuingSize = currentMap.size()) > maxQueuingCount) {
					logger.info("back pushing triggered for db indx:{}, size: {}", dbIndex, queuingSize);
					try {
						Thread.sleep(1);
					} catch(Exception e) {
					}
				}
			}

			currentMap.put(key, value);
		}
		
		/**
		 * Store a record.
		 * 
		 * @param key
		 * @param value
		 */
		public void storeOffset(long value) throws Exception {
			if(value >= 0){
				offset = value;
			}
			else{
				throw new Exception("offset can't be negative value");
			}
			
		}

		/**
		 * Get a record.
		 * 
		 * @param key
		 * @return
		 * @throws RocksDBException 
		 */
		public byte[] get(long key) throws RocksDBException {
			byte[] result = currentMap.get(key);
			if (result == null && prevMap != null) {
				result = prevMap.get(key);
			}
			if(result == null) {
			    byte[] byteKey = Longs.toByteArray(key);
			    try{
				   result =  db.get(byteKey);
			    } catch (RocksDBException e) {
				   System.out.format("caught the unexpceted exception -- %s\n", e);
			    }
			}
			return result;
			
		}
		
		public long getOffset() throws RocksDBException {		
			if (offset == -1) {
				byte[] result = db.get(offsetKey.getBytes());
				if(result == null){
					return 0;
				}
				offset = Convert.bytesToLong(result);
			}
			return offset;
			
		}

		
		/**
		 * Remove a record.
		 * 
		 * @param key
		 * @return
		 * @throws RocksDBException 
		 */
		public boolean delete(long key) throws RocksDBException  {
			currentMap.remove(key);
			if (prevMap != null) {
				prevMap.remove(key);
			}
			byte[] byteKey = Longs.toByteArray(key);
			try{
				db.remove(byteKey);
				storeCounter.decrementAndGet();
			} catch (RocksDBException e) {
			     return false;		    
			} 
		    return true;
				
		}
		
		
		/**
		 * Get total number of records.
		 * 
		 * @return
		 */
		public long count() {
			return storeCounter.get();
		}

//		/**
//		 * Get total number of records.
//		 * 
//		 * @return
//		 */
//		public long count() {

//		}

//		/**
//		 * Go through all the records with the visitor.
//		 * 
//		 * @param visitor
//		 */
//		public void iterate(Visitor visitor) {
//			
//		}
		
//		/**
//		 * Go through all the records with the visitor.
//		 * 
//		 * @param visitor
//		 */
//		public void iterateWithReadLock(Visitor visitor) {
//
//		}
		
		private void flushOffset() throws RocksDBException{
			if(offset > 0){
				try{
				    db.put(offsetKey.getBytes(), Convert.longToBytes(offset));
				} catch (RocksDBException e) {
					logger.debug("Failed to store offset for {}, Error: {}", offset);
					    
				} 
			logger.debug("successfully store offset value {}", offset);
			}

		}
		
		private void flush() throws RocksDBException {
			
            if (currentMap.size() > 0 || isClosing || forceFlush) {
				
				prevMap = currentMap;
				currentMap = new ConcurrentHashMap<Long, byte[]>(2 * writeBufferSize);

				long st = System.currentTimeMillis();
				//db.begin_transaction(false); 
				//no similar method for rocksdb here
				for (Entry<Long, byte[]> item : prevMap.entrySet()) {
					long key = item.getKey();
					byte[] value = item.getValue();
					try{
					    db.put(Convert.longToBytes(key), value);
					    storeCounter.incrementAndGet();
					} catch (RocksDBException e) {
						logger.debug("Failed to store offset for {}, Error: {}", offset);					    
					} 
				}
				if(numStores == 0 &&(flushCounter.addAndGet(1) % PER_OFFSET_FLUSH == 0 || isClosing || forceFlush))
					flushOffset();
				//db.end_transaction(true);
				logger.info("Flushed {} profiles in {}ms", prevMap.size(), System.currentTimeMillis() - st);

				prevMap = null;
				forceFlush = false;
			}
	
		}
	}
}
