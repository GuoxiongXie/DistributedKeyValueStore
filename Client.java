package edu.berkeley.cs162;

public class Client {

	public Client() {
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		KVClient<String, String> client = new KVClient<String, String>("localhost", 8080);
//		KVClient<String, String> client = new KVClient<String, String>("ec2-23-20-184-186.compute-1.amazonaws.com", 8080);
//		try {	
//			
//			//client.del("testkey1");
//			//b = client.put("testkey1", "testvalue1_2");
//			//System.out.println("second b : " + b);
//			//String getResult1 = client.get("testkey1");
//			//System.out.println("Client: get testkey1!=======test del");
//			//System.out.println(getResult1);			
//			//==============
//			
//			boolean b = client.put("testkey1", null);
//			System.out.println("first b : " + b);
//			
//			System.out.println("Client: put testkey1!");
//			String getResult = client.get("testkey1");
//			System.out.println("Client: get testkey1!=======first");
//			System.out.println(getResult);
//
//			b = client.put("testkey1", "testvalue1_2");
//			System.out.println("second b : " + b);
//			getResult = client.get("testkey1");
//			System.out.println("Client: get testkey1!=======second");
//			System.out.println(getResult);
//			
//			client.del("testkey1");
//			//b = client.put("testkey1", "testvalue1_2");
//			//System.out.println("second b : " + b);
//			getResult = client.get("testkey1");
//			//System.out.println("Client: get testkey1!=======test del");
//			System.out.println(getResult);
//			
//		} catch (KVException e) {
//			// TODO Auto-generated catch block
//			System.out.println(e.getMsg().getMsg());
//			//e.printStackTrace();
//		}
		
		try {
			String k = "key1";
			boolean b = client.put(k, "why is life so hard????");
			String getResult = client.get(k);
			System.out.println("My Result: " + getResult);
			try {
				client.del("baga!!!");
			} catch(KVException e) {
				System.out.println(e.getMsg().getMsg());
			}
		} catch (KVException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMsg().getMsg());
		}
		
		
		
	}

}
