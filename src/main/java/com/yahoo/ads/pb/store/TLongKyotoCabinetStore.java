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

import kyotocabinet.Cursor;
import kyotocabinet.DB;
import kyotocabinet.Visitor;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;

import com.google.common.base.Preconditions;
//import com.yahoo.ads.pb.platform.profile.ProfileUtil;

public class TLongKyotoCabinetStore {
	private static Logger logger = LoggerFactory.getLogger(TLongKyotoCabinetStore.class);

	private static final int writeBufferSize = 5000;
	
	private final String baseDir;
	private final int numStores;   //0 means use helix to control partition num
	private final int recordSizeAlignment;
	private final int totalRecords;
	private final long mappedMemorySize;
	private final boolean isReadOnly;
	private final Store[] stores;

	public TLongKyotoCabinetStore(
			String baseDir,
			int numStores,
			int recordSizeAlignment,
			int totalRecords,
			long mappedMemorySize) {
		
		this(baseDir, numStores, recordSizeAlignment, totalRecords, mappedMemorySize, false);
	}
	
	public TLongKyotoCabinetStore(
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
	 * Open the data base
	 */
	public void open(int partitionId) throws Exception {
		stores[partitionId] = new Store(partitionId);
		stores[partitionId].open();
	}

	/**
	 * Close all database.
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
	public void store(long key, byte[] value) throws Exception {
		int dbIndex = getDbIndex(key);
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
	 */
	public byte[] get(long key) {
		int dbIndex = getDbIndex(key);
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
	 */
	public boolean delete(long key) {
		int dbIndex = getDbIndex(key);
		if(stores[dbIndex] != null)
			return stores[dbIndex].delete(key);
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

	/**
	 * Go through all the records with the visitor.
	 * 
	 * @param visitor
	 */
	public void iterate(Visitor visitor) {
		if (visitor == null) {
			return;
		}

		for (Store store : stores) {
			if (store != null) {
				store.iterate(visitor);
			}
		}
	}
	
	/**
	 * Go through all the records with the visitor.
	 * 
	 * @param visitor
	 */
	public void iterateWithReadLock(Visitor visitor) {
		if (visitor == null) {
			return;
		}

		for (Store store : stores) {
			if (store != null) {
				store.iterateWithReadLock(visitor);
			}
		}
	}
	
	/**
	 * Go through all the records in the single store with visistor
	 * 
	 * @param visitor
	 * @param index
	 *
	 */
	public void iteratorSingleStore(Visitor visitor, int index) {
		
		Store store = stores[index];
		if (store != null) {
			store.iterateWithReadLock(visitor);
		}
	}

	private class Store {
		private final int dbIndex;
		private final DB hDb;

		private volatile ConcurrentHashMap<Long, byte[]> prevMap;
		private volatile ConcurrentHashMap<Long, byte[]> currentMap;
		private volatile ConcurrentHashMap<byte[], byte[]> offsetMap;
		private volatile boolean isClosing;
		private volatile boolean forceFlush;
		private Thread profileWriter;
		private AtomicInteger flushCounter;
		private final static int PER_OFFSET_FLUSH = 100;
		private long offset  = -1;
		private static final String offsetKey  = "offset_storage_tk";

		private java.util.Random rand = new java.util.Random();
		private static final int maxQueuingCount = 200000;
		private volatile int queuingSize = 0;

		public Store(int dbIndex) {
			this.dbIndex = dbIndex;

			// use TLINEAR
			hDb = new DB(2);
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
			int mode;
			if (isReadOnly) {
				mode = DB.OREADER;
			}
			else {
				mode = DB.OWRITER | DB.OCREATE;
			}

			StringBuilder fileNameBuilder = new StringBuilder();
			fileNameBuilder.append(baseDir).append(dbIndex);

			try{
				File baseDirFile = new File(fileNameBuilder.toString());
				if (!baseDirFile.exists()) {
					logger.info("file not found, create it:{}", fileNameBuilder.toString());
					baseDirFile.mkdir();
				} else {
					logger.info("file exists :{}", fileNameBuilder.toString());
				}
			}catch(Exception e) {
				logger.info("error mkdir {}", fileNameBuilder.toString(), e);
			}

			fileNameBuilder.append("/db_").append(dbIndex).append(".kch");
			// specifies the power of the alignment of record size
			fileNameBuilder.append("#apow=").append(recordSizeAlignment);	
			// specifies the number of buckets of the hash table
			if(numStores==0){
				fileNameBuilder.append("#bnum=").append(totalRecords * 2 / 8);
			}else{
				fileNameBuilder.append("#bnum=").append(totalRecords * 2 / numStores);
			}
			// specifies the mapped memory size
			if(numStores==0){
				fileNameBuilder.append("#msiz=").append(mappedMemorySize / 8);
			}else{
				fileNameBuilder.append("#msiz=").append(mappedMemorySize / numStores);
			}
			// specifies the unit step number of auto defragmentation
			fileNameBuilder.append("#dfunit=").append(8);

			if (!hDb.open(fileNameBuilder.toString(), mode)){
				throw new Exception(String.format("KC HDB %s open error: " + hDb.error(), 
						fileNameBuilder.toString()));
			}

			profileWriter = new Thread(new Runnable() {

				@Override
				public void run() {
					while (!isClosing) {
						flush();

						try {
							Thread.sleep(100L);
						}
						catch (InterruptedException e) { }
					}

					flush();
				}
			});
			profileWriter.setName("ProfileWriter_" + dbIndex);
			profileWriter.setDaemon(true);
			profileWriter.start();
		}

		/**
		 * Close all database.
		 */
		public void close() {
			isClosing = true;
			try {
				profileWriter.join();
			}
			catch (InterruptedException e) { }

			if (hDb != null) {
				hDb.close();
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
		 */
		public byte[] get(long key) {
			byte[] result = currentMap.get(key);
			if (result == null && prevMap != null) {
				result = prevMap.get(key);
			}

			if (result == null) {
				result = hDb.get(Convert.longToBytes(key));
			}

			return result;
		}
		
		public long getOffset() {
			if (offset == -1) {
				byte[] result = hDb.get(offsetKey.getBytes());
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
		 */
		public boolean delete(long key) {
			currentMap.remove(key);
			if (prevMap != null) {
				prevMap.remove(key);
			}

			return hDb.remove(Convert.longToBytes(key));
		}

		/**
		 * Get total number of records.
		 * 
		 * @return
		 */
		public long count() {
			if (hDb != null) {
				return hDb.count();
			}

			return 0;
		}

		/**
		 * Go through all the records with the visitor.
		 * 
		 * @param visitor
		 */
		public void iterate(Visitor visitor) {
			if (visitor == null) {
				return;
			}

			if (hDb != null) {
				Cursor cursor = hDb.cursor();
				cursor.jump();
				try {
					while (cursor.accept(visitor, true, true)) { }
				}
				finally {
					cursor.disable();
				}
			}
		}
		
		/**
		 * Go through all the records with the visitor.
		 * 
		 * @param visitor
		 */
		public void iterateWithReadLock(Visitor visitor) {
			if (visitor == null) {
				return;
			}

			if (hDb != null) {
				Cursor cursor = hDb.cursor();
				cursor.jump();
				try {
					byte[][] value = cursor.get(true);
					while (value!=null && !value[0].equals(offsetKey.getBytes())) { 
						visitor.visit_full(value[0], value[1]);
						value = cursor.get(true);
					}
				}
				finally {
					cursor.disable();
				}
			}
		}

		private void flushOffset(){
			if(offset > 0){
				if (!hDb.set(offsetKey.getBytes(), Convert.longToBytes(offset))) {
					logger.error("Failed to store offset for {}, Error: {}", offset, hDb.error());
				}else{
					logger.debug("successfully store offset value {}", offset);
				}
			}
		}
		private void flush() {
			if (currentMap.size() > 0 || isClosing || forceFlush) {
				
				prevMap = currentMap;
				currentMap = new ConcurrentHashMap<Long, byte[]>(2 * writeBufferSize);

				long st = System.currentTimeMillis();
				hDb.begin_transaction(false);
				for (Entry<Long, byte[]> item : prevMap.entrySet()) {
					long key = item.getKey();
					byte[] value = item.getValue();
					if (!hDb.set(Convert.longToBytes(key), value)) {
						logger.error("Failed to store for {}, Error: {}", key, hDb.error());
					}
				}
				if(numStores == 0 &&(flushCounter.addAndGet(1) % PER_OFFSET_FLUSH == 0 || isClosing || forceFlush))
					flushOffset();
				hDb.end_transaction(true);
				logger.info("Flushed {} profiles in {}ms", prevMap.size(), System.currentTimeMillis() - st);

				prevMap = null;
				forceFlush = false;
			}
		}
	}
}
