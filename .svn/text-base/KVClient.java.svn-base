/**
 * Client component for generating load for the KeyValue store. 
 * This is also used by the Master server to reach the slave nodes.
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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.Socket;
import java.security.InvalidKeyException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;

/**
 * This class is used to communicate with (appropriately marshalling and unmarshalling) 
 * objects implementing the {@link KeyValueInterface}.
 *
 * @param <K> Java Generic type for the Key
 * @param <V> Java Generic type for the Value
 */
public class KVClient<K extends Serializable, V extends Serializable> implements KeyValueInterface<K, V> {

	private String server = null;
	private int port = 0;
	private KVCrypt crypt = null;
	boolean hasKey = false;			   // whether or not this client has the enKey
	
	/**
	 * @param server is the DNS reference to the Key-Value server
	 * @param port is the port on which the Key-Value server is listening
	 */
	public KVClient(String server, int port) {
		this.server = server;
		this.port = port;
	}

	
	public void requestEnKey(){
		//message = Create new KVMessage of type getEnKey
		KVMessage message = new KVMessage("getEnKey");
		
		//Send message to master server
		String message_str = message.toXML();
		
		//response = KVMessage received from master server
		try {
			KVMessage response = createRequest(message_str, true);
			String enKey = response.getMsg();
			this.crypt = new KVCrypt(enKey);
			hasKey = true;
		} catch (KVException e) {
			e.printStackTrace();
		} catch (Exception e){
			e.printStackTrace();
		}
	}	

	@Override
	public boolean put(K key, V value) throws KVException {
		
		if (!hasKey) {   
			requestEnKey();
		} 
		
		try {
			String svalue = KVMessage.encodeObject(crypt.encrypt(KVMessage.encodeObject(value)));
			String skey = KVMessage.encodeObject(key);
			
			KVMessage message = new KVMessage( "putreq", skey, svalue, false );
			String message_str = message.toXML();
			KVMessage response = createRequest(message_str, false);
			return response.getStatus();
			
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		} catch (BadPaddingException e) {
			e.printStackTrace();
		} catch (IllegalBlockSizeException e) {
			e.printStackTrace();
		}
		
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public V get(K key) throws KVException {
		
		if (!hasKey) {   
			requestEnKey();
		} 		
		try {
			KVMessage message = new KVMessage( "getreq", key, true );
			String message_str = message.toXML();
			KVMessage response = createRequest(message_str, false);
			
			if( isErrorMsg(response) )
				return null;
			
			String svalue = response.getValue();
			byte[] cvalue = (byte[]) KVMessage.decodeObject(svalue);
			
			// If no such key, don't decrypt 
			if( cvalue==null ){ return null; }
			V value = (V) KVMessage.decodeObject(crypt.decrypt(cvalue));
			return value;
		
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		} catch (BadPaddingException e) {
			e.printStackTrace();
		} catch (IllegalBlockSizeException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void del(K key) throws KVException {
		KVMessage message = new KVMessage( "delreq", key, true );
		String message_str = message.toXML();
		createRequest(message_str, false);
	}
	
	// private method to create requests and return the response from the server
	private KVMessage createRequest(String msg_str, boolean enkey) throws KVException{
		
		KVMessage response = null;
		Socket socket = null;
		try{
			// Write request to server
			socket = new Socket(server, port);
			DataOutputStream out = new DataOutputStream( socket.getOutputStream() );
			out.writeBytes(msg_str);
			socket.shutdownOutput();
			
			// Receive response from server
			InputStream input = socket.getInputStream();
			response = new KVMessage( input );
			socket.close();
			
		} catch (IOException e) {
			// error creating socket
			if (socket == null)
				throw new KVException(new KVMessage("resp", null, null, "Network Error: Could not create socket"));
			
			// if exception thrown after output is shutdown then there was an error receiving the response
			if (socket.isOutputShutdown())
				throw new KVException(new KVMessage("resp", null, null, "Network Error: Could not receive data"));
			
			// otherwise there was an error sending the request to server
			else throw new KVException(new KVMessage("resp", null, null, "Network Error: Could not send data"));
		}
		
		System.out.println("----\tREQUEST: "+msg_str+" is \n\t"+"RESPONSE: "+response.toXML()+"\n");
		
		if( !enkey && isErrorMsg(response) ){
			//System.out.println("KVClient createRequest: KVException!! Error message is "+response.getMsg());
			throw new KVException(response);
		}
		
		return response;
	}
	
	private static boolean isErrorMsg( KVMessage m ){
		return m.getMsg()!=null && !m.getMsg().equals("Success");
	}
}
