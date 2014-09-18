/**
 * Master for Two-Phase Commits
 * 
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
 *
 * Copyright (c) 2012, University of California at Berkeley
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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.TreeMap;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;

public class TPCMaster<K extends Serializable, V extends Serializable> {

	/**
	 * Implements NetworkHandler to handle registration requests from
	 * SlaveServers.
	 * 
	 */
	private class TPCRegistrationHandler implements NetworkHandler {

		private ThreadPool threadpool = null;

		public TPCRegistrationHandler() {
			// Call the other constructor
			this(1);
		}

		public TPCRegistrationHandler(int connections) {
			threadpool = new ThreadPool(connections);
		}

		@Override
		public void handle(Socket client) throws IOException {
			RegistrationRunnable task = new RegistrationRunnable(client);
			try {
				threadpool.addToQueue(task);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		private class RegistrationRunnable implements Runnable {

			Socket clientSocket;

			public RegistrationRunnable(Socket client) {
				clientSocket = client;
			}

			public void run() {

				InputStream in;
				KVMessage response = null;
				KVMessage regResp = null;
				try {
					in = clientSocket.getInputStream();
					response = new KVMessage(in);
					String infoString = response.getMsg();
				
					// Create a new slaveInfo (might throw unparseable error)
					SlaveInfo slaveInfo = new SlaveInfo(infoString);
					slaveMap.put(slaveInfo.getSlaveID(), slaveInfo);

					regResp = new KVMessage("resp", "Successfully registered " + infoString);
					System.out.println("Master: Successfully registered "+clientSocket);

				} catch (KVException e) {
					regResp = e.getMsg();
				} catch (IOException e) {
					return;
				}

				// Return a Registration ACK back to Slave server
				try {
					String message = regResp.toXML();
					DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());

					out.write(message.getBytes());

					clientSocket.shutdownOutput();
					clientSocket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Data structure to maintain information about SlaveServers
	 * 
	 */
	private class SlaveInfo {
		// 64-bit globally unique ID of the SlaveServer
		private long slaveID = -1;
		// Name of the host this SlaveServer is running on
		private String hostName = null;
		// Port which SlaveServer is listening to
		private int port = -1;

		/**
		 * 
		 * @param slaveInfo
		 *            as "SlaveServerID@HostName:Port"
		 * @throws KVException
		 */
		public SlaveInfo(String slaveInfo) throws KVException {

			// String parsing of slaveInfo
			String[] atSplit = slaveInfo.split("@"); // Split into SlaveServerID and HostName:Port
			String slaveIdString = atSplit[0];

			try {
				slaveID = Long.parseLong(slaveIdString);

				String[] colonSplit = atSplit[1].split(":"); // Split into HostName and Port
				hostName = colonSplit[0];
				port = Integer.parseInt(colonSplit[1]);

			} catch (NumberFormatException e) {
				KVMessage errorMsg = new KVMessage("resp", "Registration Error: Received unparseable slave information");
				throw new KVException(errorMsg);
			}
		}

		public long getSlaveID() {
			return slaveID;
		}

		public int getPort() {
			return port;
		}

		public String getHostName() {
			return hostName;
		}

	}

	// Timeout value used during 2PC operations
	private static final int TIMEOUT_MILLISECONDS = 5000;

	// Cache stored in the Master/Coordinator Server
	private KVCache<K, V> masterCache = new KVCache<K,V>(1000);

	// Registration server that uses TPCRegistrationHandler
	private SocketServer regServer = null;

	// ID of the next 2PC operation
	private Long tpcOpId = 0L;

	// Mapping from slaveId to slaveInfo
	TreeMap<Long, SlaveInfo> slaveMap;

	// key string
	private String keyStr;
	
	/**
	 * Creates TPCMaster using SlaveInfo provided as arguments and SlaveServers
	 * actually register to let TPCMaster know their presence
	 * 
	 * @param listOfSlaves
	 *            list of SlaveServers in "SlaveServerID@HostName:Port" format
	 * @throws Exception
	 */
	public TPCMaster(String[] listOfSlaves) throws Exception {

		// Initialize and fill the slaveMap
		slaveMap = new TreeMap<Long, SlaveInfo>(new Comparator<Long>(){
			public int compare(Long l1, Long l2){
				if( l1.longValue()==l2.longValue() ){ return 0; }
				else{ return ( isLessThanUnsigned(l1.longValue(), l2.longValue()) ) ? -1 : 1; }
			}
		});
		KVCrypt crypt = new KVCrypt();
		crypt.setUp();
		keyStr = crypt.getKeystr();
		for (String infoString : listOfSlaves) {
			SlaveInfo slaveInfo = new SlaveInfo(infoString);
			slaveMap.put(slaveInfo.getSlaveID(), slaveInfo);
		}

		// Create registration server
		regServer = new SocketServer(InetAddress.getLocalHost().getHostAddress(), 9090);
	}

	public String getkeyStr() {
		return keyStr;
	}

	/**
	 * Calculates tpcOpId to be used for an operation. In this implementation it
	 * is a long variable that increases by one for each 2PC operation.
	 * 
	 * @return
	 */
	private String getNextTpcOpId() {
		tpcOpId++;
		return tpcOpId.toString();
	}

	/**
	 * Start registration server in a separate thread
	 */
	public void run() {
		Thread t = new Thread() {
			public void run() {
				try {
					regServer.connect();
					regServer.addHandler(new TPCRegistrationHandler());
					regServer.run();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
		t.start();
	}

	/**
	 * Converts Strings to 64-bit longs Borrowed from
	 * http://stackoverflow.com/questions
	 * /1660501/what-is-a-good-64bit-hash-function-in-java-for-textual-strings
	 * Adapted from String.hashCode()
	 * 
	 * @param string
	 *            String to hash to 64-bit
	 * @return
	 */
	private long hashTo64bit(String string) {
		// Take a large prime
		long h = 1125899906842597L;
		int len = string.length();

		for (int i = 0; i < len; i++) {
			h = 31 * h + string.charAt(i);
		}
		return h;
	}

	/**
	 * Compares two longs as if they were unsigned (Java doesn't have unsigned
	 * data types except for char) Borrowed from
	 * http://www.javamex.com/java_equivalents/unsigned_arithmetic.shtml
	 * 
	 * @param n1
	 *            First long
	 * @param n2
	 *            Second long
	 * @return is unsigned n1 less than unsigned n2
	 */
	private boolean isLessThanUnsigned(long n1, long n2) {
		return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
	}

	/**
	 * Find first/primary replica location
	 * 
	 * @param key
	 * @return
	 */
	private SlaveInfo findFirstReplica(K key)
	{
		if( key==null )
			return null;
		
		// 64-bit hash of the key
		long hashedKey = hashTo64bit(key.toString());

		if (slaveMap.containsKey(hashedKey)) {
			return slaveMap.get(hashedKey);
		}

		Entry<Long, SlaveInfo> successorEntry = slaveMap.higherEntry(hashedKey);

		// handle wrap around
		if (successorEntry == null) {
			successorEntry = slaveMap.firstEntry();
		}
		return (successorEntry==null) ? null : successorEntry.getValue();
	}

	/**
	 * Find the successor of firstReplica to put the second replica
	 * 
	 * @param firstReplica
	 * @return
	 */
	private SlaveInfo findSuccessor(SlaveInfo firstReplica) {

		if( firstReplica==null )
			return null;
		
		long firstReplicaID = firstReplica.getSlaveID();
		Entry<Long, SlaveInfo> successorEntry = slaveMap.higherEntry(firstReplicaID);

		// handle wrap around
		if (successorEntry == null) {
			successorEntry = slaveMap.firstEntry();
		}
		return (successorEntry==null) ? null : successorEntry.getValue();
	}

	/**
	 * Synchronized method to perform 2PC operations one after another
	 * 
	 * @param msg
	 * @param isPutReq
	 * @return True if the TPC operation has succeeded
	 * @throws KVException
	 */
	public synchronized boolean performTPCOperation(KVMessage msg, boolean isPutReq) throws KVException {

		try{
			boolean aborting = false;
			String opID = getNextTpcOpId();

			/* Check if isPutReq coincides with msg type */
			if (isPutReq && !msg.getType().equals("putreq"))	{ return false; }
			if (!isPutReq && !msg.getType().equals("delreq"))	{ return false; }
			
			/* Retrieve key and value from msg */
			K key = (K) msg.getKey();
	    	V value = (V) msg.getValue();	    	
	    	
			/* Set up slaves 1 and 2 given the key */
			SlaveInfo slave1 = findFirstReplica(key);
			SlaveInfo slave2 = findSuccessor(slave1);
			String slave1ErrorMsg = "";
			String slave2ErrorMsg = "";
			
			/* Send putreq/delreq, and await response */
			ExchangeRequestRunnable r1a = new ExchangeRequestRunnable(opID, slave1, msg);
			ExchangeRequestRunnable r2a = new ExchangeRequestRunnable(opID, slave2, msg);
			Thread t1a = new Thread(r1a);
			Thread t2a = new Thread(r2a);
			t1a.start();
			t2a.start();
			t1a.join();
			t2a.join();
			slave1ErrorMsg = r1a.getErrorMessage();
			slave2ErrorMsg = r2a.getErrorMessage();
			
			/* Send commit/abort decision, and await response */
			aborting = !r1a.getResponse() || !r2a.getResponse();
			ExchangeDecisionRunnable r1b = new ExchangeDecisionRunnable(opID, slave1, aborting);
			ExchangeDecisionRunnable r2b = new ExchangeDecisionRunnable(opID, slave2, aborting);
			Thread t1b = new Thread(r1b);
			Thread t2b = new Thread(r2b);
			t1b.start();
			t2b.start();
			t1b.join();
			t2b.join();
			slave1ErrorMsg = (r1b.getErrorMessage().equals("") ? slave1ErrorMsg : r1b.getErrorMessage());
			slave2ErrorMsg = (r2b.getErrorMessage().equals("") ? slave1ErrorMsg : r2b.getErrorMessage());
			
			/* Throw exception if aborting */
			if( aborting ){
				String totalErrorMsg = "";
				if( !slave1ErrorMsg.equals("") ){				
					totalErrorMsg += "@" + slave1.getSlaveID() + "=>" + slave1ErrorMsg;
					if( !slave2ErrorMsg.equals("") ){
						totalErrorMsg += "\n";
					}
				}
				if( !slave2ErrorMsg.equals("") ){
					totalErrorMsg += "@" + slave2.getSlaveID() + "=>" + slave2ErrorMsg;	
				}
				throw new KVException( new KVMessage("resp", totalErrorMsg) );
			}
			
			/* Update corresponding entry in cache */
			if( isPutReq )	{ masterCache.put(key, value); }
					else	{ masterCache.del(key); }
			
			return true;
		}
		catch( InterruptedException e ){
			throw new KVException( new KVMessage("resp", "Unknown Error! Please try again later.") );
		}
	}

	private class ExchangeRequestRunnable implements Runnable
	{
		boolean ready;
		SlaveInfo slave;
		String opID;
		String errorMessage;
		KVMessage msg;

		public ExchangeRequestRunnable(String opID, SlaveInfo slave, KVMessage msg) {
			this.opID = opID;
			this.slave = slave;
			this.msg = msg;
			errorMessage = "";
		}

		public void run() {
			if( slave==null )
				return;
			
			while (true) {	// Retry until successful transmission of putreq/delreq
				try {
					Socket slaveSocket = new Socket(slave.getHostName(), slave.getPort());
					KVMessage response;
					
					msg.setId(opID);
					
					DataOutputStream out = new DataOutputStream( slaveSocket.getOutputStream() );
					out.writeBytes( msg.toXML() );
					slaveSocket.shutdownOutput();
					
					TimeoutRunnable tr = new TimeoutRunnable(slaveSocket);
					Thread timeoutThread = new Thread(tr);
					timeoutThread.start();
					timeoutThread.join(TIMEOUT_MILLISECONDS);
					
					slaveSocket.close();
					tr.throwException();
					response = tr.getResponse();
					
					if (!tr.isDone()){
						errorMessage = "Timeout Error: SlaveServer "+slave.getSlaveID() +" has timed out during the first phase of 2PC";
						return;
					}
					
					if( response.getType().equals("ready") ){
						ready = true;
					}
					
					if( response.getType().equals("abort") ){
						ready = false;
						errorMessage = response.getMsg();
					}
					return;
				}
				catch (IOException e) { continue; }
				catch (ParserConfigurationException e) {
					errorMessage = "Unknown Error: ParserConfigurationException";
					return;
				}
				catch (SAXException e) {
					errorMessage = "Unknown Error: SAXException";
					return;
				}
				catch (KVException e) {
					errorMessage = e.getMsg().getMsg();
		 			return;
				} catch (Exception e) {
					errorMessage = "Unknown Error! Please try again later.";
					return;
				}
			}
		}

		public String getErrorMessage() {
			return errorMessage;
		}
		
		// true -> ready; false -> abort
		public boolean getResponse(){
			return ready;
		}
		
	}

	private class ExchangeDecisionRunnable implements Runnable
	{
		boolean aborting;
		SlaveInfo slave;
		String opID;
		String errorMessage;

		public ExchangeDecisionRunnable(String opID, SlaveInfo slave, boolean aborting) {
			this.opID = opID;
			this.slave = slave;
			this.aborting = aborting;
			errorMessage = "";
		}

		public void run() {
			if( slave==null )
				return;
			 
			while (true) {	// Retry until successful transmission of commit/abort
				try {
					Socket slaveSocket = new Socket(slave.getHostName(), slave.getPort());
					KVMessage requestMsg, response;

					if (aborting)	{ requestMsg = new KVMessage("abort", null, opID); }
							else 	{ requestMsg = new KVMessage("commit", null, opID); }

					DataOutputStream out = new DataOutputStream( slaveSocket.getOutputStream() );
					out.writeBytes( requestMsg.toXML() );
					slaveSocket.shutdownOutput();
					
					TimeoutRunnable tr = new TimeoutRunnable(slaveSocket);
					Thread timeoutThread = new Thread(tr);
					timeoutThread.start();
					timeoutThread.join(TIMEOUT_MILLISECONDS);
					
					tr.throwException();
					response = tr.getResponse();
					
					if (!tr.isDone()){
						continue;
					}
					slaveSocket.close();
					
					if( !response.getType().equals("ack") ){
						errorMessage = response.getMsg();
					}
					return;
				}
				catch (IOException e) { continue; }
				catch (ParserConfigurationException e) {
					errorMessage = "Unknown Error: ParserConfigurationException";
					return;
				}
				catch (SAXException e) {
					errorMessage = "Unknown Error: SAXException";
					return;
				}
			 	catch (KVException e) {
					errorMessage = e.getMsg().getMsg();
					return;
				} catch (Exception e) {
					errorMessage = "Unknown Error! Please try again later.";
					return;
				}
			}
		}

		public String getErrorMessage() {
			return errorMessage;
		}
	}
	
	private class TimeoutRunnable implements Runnable {
		
		Socket slaveSocket;
		KVMessage response;
		Exception runnableException;
		boolean done;
		
		public TimeoutRunnable(Socket slaveSocket){
			this.slaveSocket = slaveSocket;
			done = false;
		}
		public void run(){
			
			try {
				response = new KVMessage( slaveSocket.getInputStream() );
				done = true;
			} catch (Exception e) {
				runnableException = e;
			}
		}
		
		public void throwException() throws Exception{
			if (runnableException != null)
				throw runnableException;
		}
		public KVMessage getResponse(){
			return response;
		}
		public boolean isDone(){
			return done;
		}
	}
	
	
	
	/**
	 * Perform GET operation in the following manner: - Try to GET from
	 * first/primary replica - If primary succeeded, return Value - If primary
	 * failed, try to GET from the other replica - If secondary succeeded,
	 * return Value - If secondary failed, return KVExceptions from both
	 * replicas
	 * 
	 * @param msg
	 *            Message containing Key to get
	 * @return Value corresponding to the Key
	 * @throws KVException
	 */
	public V handleGet(KVMessage msg) throws KVException {

		if (!msg.getType().equals("getreq")) {
			KVMessage errorMsg = new KVMessage("resp", "handleGet was given a non-getreq KVMessage");
			throw new KVException(errorMsg);
		}

		K key = (K) msg.getKey();
		
		/* Try our cache */
		V value = masterCache.get(key);
		if (value != null) {
			return value;
		}

		/* Try to GET from first/primary replica */
		String totalErrorMsg = "";
		SlaveInfo slave = findFirstReplica(key);
		if( slave==null )
			return null;
		Socket slaveSocket = null;
		try{
			slaveSocket = new Socket(slave.getHostName(), slave.getPort());
			KVMessage requestMsg = new KVMessage("getreq", key, false);
			DataOutputStream out = new DataOutputStream( slaveSocket.getOutputStream() );
			out.writeBytes( requestMsg.toXML() );
			slaveSocket.shutdownOutput();
			KVMessage response = new KVMessage( slaveSocket.getInputStream() );
			if( response.getMsg()!=null ){
				throw new KVException(response);
			}
			value = (V) response.getValue();
			slaveSocket.close();
		} catch (KVException e) {
			totalErrorMsg += "@" + slave.getSlaveID() + "=>" + e.getMsg().getMsg();
		} catch( IOException e ){
			if( slaveSocket==null ){
				totalErrorMsg += "@" + slave.getSlaveID() + "=>" + "Network Error: Could not create socket";	
			} else if( slaveSocket.isOutputShutdown() ){
				totalErrorMsg += "@" + slave.getSlaveID() + "=>" + "Network Error: Could not receive data";
			} else{
				totalErrorMsg += "@" + slave.getSlaveID() + "=>" + "Network Error: Could not send data";
			}
		}
		
		/* Primary succeeded */
		if (value != null && !value.equals(KVMessage.encodeObject(null))) {
			masterCache.put(key, value);
			return value;
		}

		/* Primary failed; try to GET from other replica */
		slave = findSuccessor(slave);
		if( slave==null )
			return null;
		slaveSocket = null;
		try{
			slaveSocket = new Socket(slave.getHostName(), slave.getPort());
			KVMessage requestMsg = new KVMessage("getreq", key, false);
			DataOutputStream out = new DataOutputStream( slaveSocket.getOutputStream() );
			out.writeBytes( requestMsg.toXML() );
			slaveSocket.shutdownOutput();
			KVMessage response = new KVMessage( slaveSocket.getInputStream() );
			if( response.getMsg()!=null ){
				throw new KVException(response);
			}
	    	value = (V) response.getValue();
			slaveSocket.close();
		} catch (KVException e) {
			if( !totalErrorMsg.equals("") ){ totalErrorMsg += "\n"; }
			totalErrorMsg += "@" + slave.getSlaveID() + "=>" + e.getMsg().getMsg();
		} catch( IOException e ){
			if( !totalErrorMsg.equals("") ){ totalErrorMsg += "\n"; }
			if( slaveSocket==null ){
				totalErrorMsg += "@" + slave.getSlaveID() + "=>" + "Network Error: Could not create socket";	
			} else if( slaveSocket.isOutputShutdown() ){
				totalErrorMsg += "@" + slave.getSlaveID() + "=>" + "Network Error: Could not receive data";
			} else{
				totalErrorMsg += "@" + slave.getSlaveID() + "=>" + "Network Error: Could not send data";
			}
		}
		
		if( !totalErrorMsg.equals("") ){
			throw new KVException( new KVMessage("resp", totalErrorMsg) );
		}

		masterCache.put(key, value);
		return value;
	}
}
