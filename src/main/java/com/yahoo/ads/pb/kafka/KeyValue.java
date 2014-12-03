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

package com.yahoo.ads.pb.kafka;

import java.util.Arrays;


public class KeyValue {

	public int partition;
	public long key;
	public long seqId;
	public byte[] value;

	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (! (obj instanceof KeyValue)) return false;

		KeyValue that = (KeyValue)obj;

		return (partition==that.partition) && (key == that.key) && (seqId == that.seqId) && Arrays.equals(value, that.value);
	}
}
