package com.yahoo.ads.pb.store;

import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;

import com.yahoo.ads.pb.kafka.KeyValue;

import kyotocabinet.Cursor;

public class PistachiosTkIterator implements Iterator{
	public volatile static Map<Long,PistachiosTkIterator> iteratorMap = new WeakHashMap<Long,PistachiosTkIterator>();
	private Cursor cursor;
	 
	public static PistachiosTkIterator getPistachiosTkIterator(long id){
		if(iteratorMap.get(id) == null){
			synchronized(iteratorMap){
				if(iteratorMap.get(id) == null){
					iteratorMap.put(id, new PistachiosTkIterator());
				}
			}
		}
		return iteratorMap.get(id);
	}
	
	public boolean isCursorSet() {
		return cursor == null ? false : true;
	}
	public void setCursor(Cursor cursor) {
		this.cursor = cursor;
		cursor.jump();
	}
	
	@Override
    public boolean hasNext() {
		if(cursor.step()){
			cursor.step_back();
			return true;
		}else{
			return false;
		}
    }

	@Override
    public Object next() {
		if(cursor.step()){
			byte[] key =  cursor.get_key(false);
			byte[] value = cursor.get_value(false);
			KeyValue keyValue = new KeyValue();
			keyValue.key = key;
			keyValue.value = value;
			return keyValue;
		}
	    return null;
    }

	@Override
    public void remove() {
		cursor.remove();
    }

	public void jump(byte[] key){
		cursor.jump(key);
	}
}
