/**
 * 
 * XML Parsing library for the key-value store
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.*;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;


/**
 * This is the object that is used to generate messages the XML based messages 
 * for communication between clients and servers. Data is stored in a 
 * marshalled String format in this object.
 */
public class KVMessage implements Serializable {
	private static final long serialVersionUID = 6473128480951955693L;

	private String msgType = null;
	private String key = null;
	private String value = null;
	private String status = null;
	private boolean statusSet;
	private String message = null;
	private String tpcOpId = null;
	
	/*
	 *  1) 2PC putreq/delreq
	 *  2) 2PC log - ready for put/del
	 */
	public <K,V> KVMessage( String t, K k, V v, String m, String opId, boolean encode ) throws KVException{
		msgType = t;
		key = (String) (encode ? encodeObject(k) : k);
		value = (String) (encode ? encodeObject(v) : v);
		message = m;
		statusSet = false;
		tpcOpId = opId;
	}
	
	/*
	 * 1) 2PC ready/abort response
	 * 2) 2PC commit/abort decision
	 * 3) 2PC ack
	 * 4) 2PC log - commit/abort
	 */
	public KVMessage(String t, String m, String opId) {
		msgType = t;
		message = m;
		tpcOpId = opId;
	}
	
	// for getEnKey
	public KVMessage(String t) {
		msgType = t;
	}
	
	/*
	 * 1) slave registration,
	 * 2) slave registration ack,
	 * 3) encryption key response
	 * 4) Error messages!
	 */
	public KVMessage(String t, String m) {
		msgType = t;
		message = m;
	}
	
	// for put response
	public <K,V> KVMessage( String t, K k, V v, String m, boolean s ) throws KVException{
		msgType = t;
		key = encodeObject(k);
		value = encodeObject(v);
		message = m;
		statusSet = true;
		status = (s ? "True" : "False");
	}
	
	// for get / del / error response
	public <K,V> KVMessage( String t, K k, V v, String m ) throws KVException{
		msgType = t;
		key = encodeObject(k);
		value = encodeObject(v);
		statusSet = false;
		message = m;
	}
		
	// for put request
	public <K,V> KVMessage( String t, K k, V v, boolean encode ) throws KVException{	
		if (k == null) {
			KVMessage msgs = new KVMessage ("resp", null, null, "Empty key");
			throw new KVException(msgs);			
		}
		if (v == null) {
			KVMessage msgs = new KVMessage ("resp", null, null, "Empty value");
			throw new KVException(msgs);				
		}
		msgType = t;
		key = (String) (encode ? encodeObject(k) : k);
		value = (String) (encode ? encodeObject(v) : v);
		statusSet = false;
	}
	
	// for get / del request
	public <K,V> KVMessage(String t, K k, boolean encode ) throws KVException{
		msgType = t;
		key = (String) (encode ? encodeObject(k) : k);
		statusSet = false;
	}
	
	/* Hack for ensuring XML libraries does not close input stream by default.
	 * Solution from http://weblogs.java.net/blog/kohsuke/archive/2005/07/socket_xml_pitf.html */
	private class NoCloseInputStream extends FilterInputStream {
	    public NoCloseInputStream(InputStream in) {
	        super(in);
	    }
	    
	    public void close() {} // ignore close
	}
	
	// 
	public KVMessage(InputStream input) throws KVException {

		try{
			DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			NoCloseInputStream ncInput = new NoCloseInputStream(input);
			Document doc = db.parse( ncInput );
			
			msgType = getAttributeValueByTagName(doc, "KVMessage", "type");
			key = getValueByTagName(doc, "Key");
			value = getValueByTagName(doc, "Value");
			tpcOpId = getValueByTagName(doc, "TPCOpId");
			String receivedStatus = getValueByTagName(doc, "Status");
			if( receivedStatus!=null ){
				statusSet = true;
				status = receivedStatus;
			}
			message = getValueByTagName(doc, "Message");
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e){
			throw new KVException(new KVMessage("resp", "XML Error: Received unparseable message"));
		} catch (SAXException e) {
			throw new KVException(new KVMessage("resp", "XML Error: Received unparseable message"));
		} catch (Exception e){
			e.printStackTrace();
		}
	}

	/*
	 *  *Assumes that there is only one tag with the given tag name;
	 *  Otherwise returns the value in the first tag
	 */
    private static String getValueByTagName( Document doc, String tagName )
    {
    	NodeList name = doc.getElementsByTagName( tagName );
		Node node = name.item(0);
		if ( node != null ) {
			node = node.getFirstChild();
			return (node==null) ? null : node.getNodeValue();
		}
		return null;
    }
    
    /*
     * *Assumes that there is only one tag with the given tag name, and one attribute in that tag;
     * Otherwise returns the value in the first tag, and the first attribute value
     */
    private static String getAttributeValueByTagName( Document doc, String tagName, String attribute )
    {
    	NodeList name = doc.getElementsByTagName( tagName );
		Element node = (Element) name.item(0);
		if ( node != null ) {
			return node.getAttribute(attribute);
		}
		return null;
    }	
	
	// Escapes a string to UTF-8 format
	private static String escape(String s){
		return s.replaceAll("&","&amp;")
                .replaceAll("<", "&lt;")
                .replaceAll(">", "&gt;")
                .replaceAll("\"", "&quot;")
                .replaceAll("'", "&apos;");
	}
	
	public String getValue( ) {
		return value;
	}

	public boolean getStatus( ) {
			return (status.equals("True"));
	}  

	public String getMsg( ) {
			return message;
	}	
	
	public String getType() {
		return msgType;
	}
	
	public String getKey() {
		return key;
	}
    
	public String getId() {
		return tpcOpId;
	}
	
	public void setId(String id) {
		tpcOpId = id;
	}
    
	
	/**
	 * Generate the XML representation for this message.
	 * @return the XML String
	 */
	public String toXML() {
		// implement me
		
		String xmlString = null;
		
		try{
		
			DocumentBuilderFactory dbfac = DocumentBuilderFactory.newInstance();
	        DocumentBuilder docBuilder = dbfac.newDocumentBuilder();
	        Document doc = docBuilder.newDocument();
	        
	        Element root = doc.createElement("KVMessage");
	        root.setAttribute("type", msgType);
	        doc.appendChild(root);
	        
	        if (key != null){
		        Element element = doc.createElement("Key");
				Text text = doc.createTextNode( escape(key) );
				root.appendChild(element);
				element.appendChild(text);
	        }
			
	        if (value != null){
		        Element element = doc.createElement("Value");
				Text text = doc.createTextNode( escape(value) );
				root.appendChild(element);
				element.appendChild(text);
	        }
	        
	        if (statusSet){
	        	Element element = doc.createElement("Status");
				Text text = doc.createTextNode( status );
				root.appendChild(element);
				element.appendChild(text);
	        }
	        
	        if (message != null){
	        	Element element = doc.createElement("Message");
				Text text = doc.createTextNode(message);
				root.appendChild(element);
				element.appendChild(text);
	        }
	        
	        if (tpcOpId != null){
	        	Element element = doc.createElement("TPCOpId");
				Text text = doc.createTextNode(tpcOpId);
				root.appendChild(element);
				element.appendChild(text);
	        }
	        
	        doc.setXmlStandalone(true);
			
	        //Convert document to String
	        TransformerFactory transfac = TransformerFactory.newInstance();
	        Transformer transformer = transfac.newTransformer();
	        ByteArrayOutputStream out = new ByteArrayOutputStream();
	        StreamResult result = new StreamResult(out);
	        DOMSource source = new DOMSource(doc);
	        transformer.transform(source, result);
	        	        
	        xmlString = out.toString();
	        
		} catch (Exception e){
			e.printStackTrace();
		}
		return xmlString;		
	}
	
	/**
	 * Encode Object to base64 String 
	 * @param obj
	 * @return
	 */
	public static String encodeObject(Object obj) throws KVException {
        String encoded = null;
        try{
            ByteArrayOutputStream bs = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(bs);
            os.writeObject(obj);
            byte [] bytes = bs.toByteArray();
            encoded = DatatypeConverter.printBase64Binary(bytes);
            bs.close();
            os.close();
        } catch(IOException e) {
            throw new KVException(new KVMessage("resp", "Unknown Error: Error serializing object"));
        }
        return encoded;
	}
	
	/**
	 * Decode base64 String to Object
	 * @param str
	 * @return
	 */
	public static Object decodeObject(String str) throws KVException {
		Object obj = null;
		try{
	        byte[] decoded = DatatypeConverter.parseBase64Binary(str);
	        ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(decoded));
	        obj = is.readObject();
	        is.close();
		} catch(IOException e) {
	        throw new KVException(new KVMessage("resp", "Unknown Error: Unable to decode object"));
		}
		catch (ClassNotFoundException e) {
	        throw new KVException(new KVMessage("resp", "Unknown Error: Decoding object class not found"));
		}
		return obj;
	}
}
