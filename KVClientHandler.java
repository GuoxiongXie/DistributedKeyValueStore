/**
 * Handle client connections over a socket interface
 * 
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections. 
 * It uses a threadpool to ensure that none of it's methods are blocking.
 *
 * @param <K> Java Generic type for the Key
 * @param <V> Java Generic type for the Value
 */
public class KVClientHandler<K extends Serializable, V extends Serializable> implements NetworkHandler {
	private KeyServer<K, V> keyserver = null;
	private ThreadPool threadpool = null;
	private TPCMaster<K, V> tpcMaster = null;
	
	public KVClientHandler(KeyServer<K, V> keyserver) {
		initialize(keyserver, 1);
	}

	public KVClientHandler(KeyServer<K, V> keyserver, int connections) {
		initialize(keyserver, connections);
	}

	private void initialize(KeyServer<K, V> keyserver, int connections) {
		this.keyserver = keyserver;
		threadpool = new ThreadPool(connections);	
	}
	
	public KVClientHandler(KeyServer<K, V> keyserver, TPCMaster<K, V> tpcMaster) {
		initialize(keyserver, 1, tpcMaster);
	}

	public KVClientHandler(KeyServer<K, V> keyserver, int connections, TPCMaster<K, V> tpcMaster) {
		initialize(keyserver, connections, tpcMaster);
	}

	private void initialize(KeyServer<K, V> keyserver, int connections, TPCMaster<K, V> tpcMaster) {
		this.keyserver = keyserver;
		threadpool = new ThreadPool(connections);
		this.tpcMaster = tpcMaster; 
	}

	/* (non-Javadoc)
	 * @see edu.berkeley.cs162.NetworkHandler#handle(java.net.Socket)
	 */
	@Override
	public void handle(Socket client) throws IOException {
		
		// implement me
		try {
			KVMessage response = null;
			KVRunnable<K, V> task;
			InputStream in = client.getInputStream();
			try {
				response = new KVMessage(in);
			} catch (KVException e) {
				response = e.getMsg();
			}
			task = new KVRunnable<K, V>(client, tpcMaster, response);
			threadpool.addToQueue(task);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
class KVRunnable<K extends Serializable, V extends Serializable> implements Runnable{
	Socket clientSocket;
	TPCMaster<K, V> tpcMaster;
	KVMessage requestMsg;
	
    public KVRunnable( Socket client, TPCMaster<K, V> ks, KVMessage msg ){
        clientSocket = client;
        tpcMaster = ks;
        requestMsg = msg;
    }
    
	@Override
	public void run() {
		K key;
		V value;
		String message;
		String type = requestMsg.getType();
		KVMessage response = null;
		
		if (type.equals("getreq")) {
			
			// Get, and populate response
			try {
				key = (K) requestMsg.getKey();
				value = tpcMaster.handleGet(requestMsg);		
				response = new KVMessage(type, key, value, false);
			} catch (KVException e) {
				response = e.getMsg();
			}

			// SHOULD NEVER HAPPEN
			if( response==null ) return;
			
			// Send XML version of response back to client
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
		if (type.equals("putreq")) {
			try {
				if( requestMsg.getKey().equals("") ){
					response = new KVMessage ("resp", "Empty key");
				}
				else if( requestMsg.getValue().equals("") ){
					response = new KVMessage ("resp", "Empty value");
				}
				else{
					boolean overwrite = tpcMaster.performTPCOperation(requestMsg, true);
					response = new KVMessage("resp", null, null, "Success", overwrite);
				}
			} catch (KVException e) {
				response = e.getMsg();				
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
		if (type.equals("delreq")) {
			try {
				tpcMaster.performTPCOperation(requestMsg, false);
				// create successful get response KVMessage named response...
				response = new KVMessage("resp", "Success");
			} catch (KVException e) {
				response = e.getMsg();	
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
        if(type.equals("getEnKey")){
        	//Create new KVMessage with TPCMaster.crypt.keyStr as the msg
        	String enkeys = tpcMaster.getkeyStr();
        	response = new KVMessage("resp", enkeys);
        	
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
	}	
}

