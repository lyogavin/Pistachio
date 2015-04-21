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

import java.util.concurrent.Future;
import com.yahoo.ads.pb.exception.*;

public interface PistachiosClientImpl {
    public byte[] lookup(byte[] id, boolean callback) throws MasterNotFoundException, ConnectionBrokenException, Exception;
    public boolean store(byte[] id, byte[] value, boolean callback) throws MasterNotFoundException, ConnectionBrokenException, Exception;
    public boolean processBatch(byte[] id, List<byte[]> events) throws MasterNotFoundException, ConnectionBrokenException, Exception;
    public void close();

    public Map<byte[], byte[]> multiLookup(List<byte[]> ids, boolean callback) throws MasterNotFoundException, ConnectionBrokenException, Exception;

    public Future<Map<byte[], byte[]>> multiLookupAsync(List<byte[]> ids, boolean callback) throws MasterNotFoundException, ConnectionBrokenException, Exception;
    public Future<Map<byte[], Boolean>> multiProcessAsync(Map<byte[], byte[]> events) throws MasterNotFoundException, ConnectionBrokenException, Exception;
    public Future<Boolean> storeAsync(byte[] id, byte[] value) throws MasterNotFoundException, ConnectionBrokenException, Exception;
    public boolean delete(byte[] id) throws MasterNotFoundException, ConnectionBrokenException;
    public PistachioIterator iterator(long partitionId);
}
