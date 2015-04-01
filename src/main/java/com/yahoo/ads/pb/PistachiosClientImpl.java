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


package com.yahoo.ads.pb;
import java.util.List;
import java.util.Map;

import com.google.common.util.concurrent.ListenableFuture;
import com.yahoo.ads.pb.exception.*;

public interface PistachiosClientImpl {
	public byte[] lookup(byte[] id) throws MasterNotFoundException, Exception;
	public boolean store(byte[] id, byte[] value, boolean callback) throws MasterNotFoundException, ConnectionBrokenException;
    public boolean processBatch(byte[] id, List<byte[]> events) throws MasterNotFoundException, ConnectionBrokenException;
    public void close();
    
	public Map<byte[], byte[]> multiLookup(List<byte[]> ids) throws Exception;
	
	public Map<byte[], ListenableFuture<byte[]>> multiLookupAsync(List<byte[]> ids);
	public Map<byte[], ListenableFuture<Boolean>> multiProcessAsync(Map<byte[], byte[]> events);
	public ListenableFuture<Boolean> storeAsync(byte[] id, byte[] value);
}
