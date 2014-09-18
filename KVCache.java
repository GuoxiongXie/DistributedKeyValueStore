/**
 * Implementation of an LRU Cache (copied from the Internet)
 * 
 * Copyright (c) 2011, University of California at Berkeley
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *  * Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of University of California, Berkeley nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *    
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED. IN NO EVENT SHALL PRASHANTH MOHAN BE LIABLE FOR ANY
 *  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package edu.berkeley.cs162;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An LRU cache which has a fixed maximum number of elements (cacheSize).
 * If the cache is full and another entry is added, the LRU (least recently used) entry is dropped.
 */
public class KVCache<K extends Serializable, V extends Serializable> implements KeyValueInterface<K, V>{
	private int cacheSize;
	private LinkedHashMap<K,V> cache;
	private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock read = readWriteLock.readLock();
	private final Lock write = readWriteLock.writeLock();

	/**
	 * Creates a new LRU cache.
	 * @param cacheSize the maximum number of entries that will be kept in this cache.
	 */
	public KVCache (int cacheSize) {
		this.cacheSize = cacheSize;
		cache = new LinkedHashMap<K,V>(cacheSize);
	}

	/**
	 * Retrieves an entry from the cache.
	 * The retrieved entry becomes the MRU (most recently used) entry.
	 * @param key the key whose associated value is to be returned.
	 * @return the value associated to this key, or null if no value with this key exists in the cache.
	 */
	public V get (K key) {
		read.lock();
		V entry = cache.get(key);
		if (entry == null){
			read.unlock();
			return null;
		}
		read.unlock();
		
		write.lock();
		V value = cache.get(key);
		cache.remove(key);
		cache.put(key, value);
		write.unlock();
		return entry;
	}

	/**
	 * Adds an entry to this cache.
	 * The new entry becomes the MRU (most recently used) entry.
	 * If an entry with the specified key already exists in the cache, it is replaced by the new entry.
	 * If the cache is full, the LRU (least recently used) entry is removed from the cache.
	 * @param key    the key with which the specified value is to be associated.
	 * @param value  a value to be associated with the specified key.
	 * @return 
	 */
	public boolean put (K key, V value) {
		write.lock();
		if(cache.get(key) != null){ 					//	overwrite/move to front
			cache.remove(key);
			cache.put(key,value);
			write.unlock();
			return true;
		}
		
		if(cache.size() < cacheSize){ 		//	unique/space for new entries
			cache.put(key,value);
		}
		else {											//	unique/delete LRU to make space
			Iterator<K> i = cache.keySet().iterator(); 
		    cache.remove(i.next());
			cache.put(key, value);
		}
		write.unlock();
		return false;
	}

	/**
	 * Removes an entry to this cache.
	 * @param key the key with which the specified value is to be associated.
	 */
	public void del (K key) {
		write.lock();
		cache.remove(key);
		write.unlock();
	}
	
	public void printAll(){
		read.lock();
		System.out.println("====================== printAll KVCache =====================");
		Iterator<K> i = cache.keySet().iterator();
		while( i.hasNext() ){
			K key = i.next();
			System.out.println("cache.get("+key+"): "+cache.get(key));
		}
		System.out.println("=====================================================");
		read.unlock();
	}
	
} // end class LRUCache