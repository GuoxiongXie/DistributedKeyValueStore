/**
 * Handle TPC connections over a socket interface
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
import java.net.Socket;
import java.util.ArrayList;

/**
 * Implements NetworkHandler to handle 2PC operation requests from the Master/
 * Coordinator Server
 *
 */
public class TPCMasterHandler<K extends Serializable, V extends Serializable> implements NetworkHandler {
	private KeyServer<K, V> keyserver = null;
	private ThreadPool threadpool = null;
	private TPCLog<K, V> tpcLog = null;

	public TPCMasterHandler(KeyServer<K, V> keyserver) {
		this(keyserver, 1);
	}

	public TPCMasterHandler(KeyServer<K, V> keyserver, int connections) {
		this.keyserver = keyserver;
		threadpool = new ThreadPool(connections);	
	}

	@Override
	public void handle(Socket client) throws IOException {
		// implement me
		try {
			KVMessage response = null;
			MHRunnable<K, V> task;
			InputStream in = client.getInputStream();
			response = new KVMessage(in);						
			task = new MHRunnable<K,V>(client, keyserver, response, tpcLog);
			threadpool.addToQueue(task);
		} catch (KVException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	class MHRunnable<K extends Serializable, V extends Serializable> implements Runnable{
		Socket clientSocket;
		KeyServer<K, V> keyServer;
		KVMessage requestMsg;
		TPCLog<K, V> log;
		MHRunnable( Socket client, KeyServer ks, KVMessage msg,TPCLog serverlog ){
			clientSocket = client;
			keyServer = ks;
			requestMsg = msg;
			log = serverlog;
		}		
		public void run() {
			
			K key = (K) requestMsg.getKey();
			V value = (V) requestMsg.getValue();
			String message;
			String type = requestMsg.getType();
			KVMessage response = null;
			
			if (type.equals("putreq") || type.equals("delreq")) {
				try {
					KVMessage logMsg = new KVMessage("ready", key, value, type, requestMsg.getId(), false);
					log.appendAndFlush(logMsg);
					response = new KVMessage("ready", null, requestMsg.getId());
					
					if (type.equals("delreq")){
						try{
							value = keyServer.get(key);
						}catch (KVException e) {
							response = new KVMessage("abort", e.getMsg().getMsg(), requestMsg.getId());
						}
					}
				} catch (Exception e) {
					response = new KVMessage("abort", e.getMessage(), requestMsg.getId());
				}
				
				// SHOULD NEVER HAPPEN
				if( response==null ) return;
				
				// transfer message to XML form and sent it back to client.
				try {
					message = response.toXML();
					DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream() );
					out.write(message.getBytes());
					clientSocket.shutdownOutput();
					clientSocket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			
			if (type.equals("commit"))
			{
			    ArrayList<KVMessage> entries = log.getEntries();
	        	KVMessage operation = null;
	        	
	        	// Iterate backwards to find corresponding ready message
	        	for( int i=entries.size()-1; i>=0; i-- ){
	        		operation = entries.get(i);
	        		if( operation.getId().equals(requestMsg.getId())
	        				&& operation.getType().equals("ready"))
	        			break;
	        	}

	        	// If no "ready" operation exists with the same opID, forget it
	        	if( operation==null )
	        		return;
	            
	            // Get key and value for put/del
	            key = (K) operation.getKey();
		    	value = (V) operation.getValue();
			    
		    	// Keep putting / deleting until success
			    while (true) {
			        try {
			            if ( operation.getMsg().equals("putreq")) { keyServer.put(key, value); }
			            if ( operation.getMsg().equals("delreq")) { keyServer.del(key); }
						// Success!
			            break;
			        } catch ( KVException e ) {
			        	if( !e.getMsg().getMsg().equals("IO Error") )
			        		break;
			        }
			    }
			    
			    // Write commit message to log
	            log.appendAndFlush(requestMsg);
			    
			    // Send ack back to master
			    try{
		            response = new KVMessage("ack", null, requestMsg.getId());
					message = response.toXML();

					DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream() );
					out.write(message.getBytes());
					clientSocket.shutdownOutput();
					clientSocket.close();
			    } catch ( IOException e ){	// Assuming that master will resend commit msg on timeout
		        	return;
		        }
			}
			
			if (type.equals("abort")) {
				
				// Write abort message to log
	            log.appendAndFlush(requestMsg);
				
				// Send ack back to master
				try {
		            response = new KVMessage("ack", null, requestMsg.getId());	
					message = response.toXML();
					DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream() );
					out.write(message.getBytes());
					clientSocket.shutdownOutput();
					clientSocket.close();
					// Ack sent
					
		        } catch ( IOException e ){	// Assuming that master will resend abort msg on timeout
		        	return;
		        }
			}	
			
			if (type.equals("getreq")) {
				// Get, and populate response
				try {
					key = (K) requestMsg.getKey();
					value = keyServer.get(key);
					response = new KVMessage("resp", key, value, false);
				} catch (KVException e) {
					response = e.getMsg();
				}

				// SHOULD NEVER HAPPEN
				if( response==null ) return;
				
				// Send XML version of response back master
				try{
					message = response.toXML();
					DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream() );
					out.write(message.getBytes());
					clientSocket.shutdownOutput();
					clientSocket.close();
				} catch( IOException e ){
					e.printStackTrace();
				}								
			}												
		}
	}	
	
	
	/**
	 * Set TPCLog after it has been rebuilt
	 * @param tpcLog
	 */
	public void setTPCLog(TPCLog<K, V> tpcLog) {
		this.tpcLog  = tpcLog;
	}

}
