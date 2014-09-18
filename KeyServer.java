/**
 * Slave Server component of a KeyValue store
 * 
 * @author Prashanth Mohan (http://www.cs.berkeley.edu/~prmohan)
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class defines the salve key value servers. Each individual KeyServer 
 * would be a fully functioning Key-Value server. For Project 3, you would 
 * implement this class. For Project 4, you will have a Master Key-Value server 
 * and multiple of these slave Key-Value servers, each of them catering to a 
 * different part of the key namespace.
 *
 * @param <K> Java Generic Type for the Key
 * @param <V> Java Generic Type for the Value
 */
public class KeyServer<K extends Serializable, V extends Serializable> implements KeyValueInterface<K, V> {
	private KVStore<K, V> dataStore = null;
	private KVCache<K, V> dataCache = null;
	private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock read = readWriteLock.readLock();
	private final Lock write = readWriteLock.writeLock();
	
	
	/**
	 * @param cacheSize number of entries in the data Cache.
	 */
	public KeyServer(int cacheSize) {
		// implement me
		dataStore = new KVStore( );
		dataCache = new KVCache( cacheSize );	
	}
	
	public boolean put(K key, V value) throws KVException {
		
		write.lock();
		boolean status;
		if (key.toString().getBytes().length > 256) {
			KVMessage msg = new KVMessage ("resp", "Over sized key");
			write.unlock();
			throw new KVException(msg);
		}
		if (value.toString().getBytes().length > 128000) {
			KVMessage msg = new KVMessage ("resp", "Over sized value");
			write.unlock();
			throw new KVException(msg);			
		}
		try {
			status = (dataStore.get(key) != null);
			dataStore.put(key, value); //write through to store
			dataCache.put(key, value); //also put in cache
			write.unlock();
			return status;
		} catch (KVException e) {
			KVMessage msg = new KVMessage ("resp", "IO Error");
			write.unlock();
			throw new KVException(msg);				
		}
	}
	
	public V get (K key) throws KVException {
		// implement me
		read.lock();
        
        //if key in cache, then get it from cache
		V value = dataCache.get(key);
		if (value != null) {
			read.unlock();
			return value;
		}
        //key not in cache
		try {
			value = dataStore.get(key);
		} catch (KVException e) {
			KVMessage msg = new KVMessage ("resp", "IO Error");
			read.unlock();
			throw new KVException(msg);				
		}
		
		if (value == null) {
			KVMessage msg = new KVMessage ("resp", "Does not exist");
			read.unlock();
			throw new KVException(msg);
		}
		dataCache.put(key, value);  //get will affect the cache too
		read.unlock();
		return value;
	}

	@Override
	public void del(K key) throws KVException {
		// implement me
		write.lock();
		V value = dataCache.get(key);
		if (value != null) {
			dataCache.del(key);		//also del in cache
		}
        
		try {
			value = dataStore.get(key);
		} catch (KVException e) {
			KVMessage msg = new KVMessage ("resp", "IO Error");
			write.unlock();
			throw new KVException(msg);				
		}
		if (value == null) {
			KVMessage msg = new KVMessage ("resp", "Does not exist");
			write.unlock();			//read.unlock();
			throw new KVException(msg);			
		}
		try {
			dataStore.del(key);
		} catch (KVException e) {
			KVMessage msg = new KVMessage ("resp", "IO Error");
			write.unlock();
			throw new KVException(msg);			
		}
		write.unlock();		
	}
}


