package edu.buffalo.cse.cse486586.simpledynamo;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.R.attr.port;
import static android.R.attr.syncable;
import static android.R.attr.theme;
import static java.lang.Integer.parseInt;

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = "Content Provider";
	public int ServerPort = 10000;
	static ConcurrentHashMap<String, String> hm;
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	Node n = new Node();
	MatrixCursor cursor= null;
	private String DELIMITER = "@#@#@";
	private String DELIMITER2 = "@@@";
	private String URI = "edu.buffalo.cse.cse486586.simpledynamo.provider";
	Uri uri = new Uri.Builder().scheme("content").authority(URI).build();
	boolean recovery;
	boolean delete = true;

	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		String[] selection_arr = selection.split("##");
		String delete_string = "";
		if(selection_arr.length==1){
			delete_string = selection_arr[0];
		}
		else {
			if (selection_arr[2].equals(String.valueOf(Integer.parseInt(get_port())/2)))
				return 1;
			delete_string = selection_arr[1];
		}
		if(delete_string.equals("@")){
			hm.clear();
			return 1;
		}
		else{
			if(delete_string.equals("*")){
				hm.clear();
				String del_message = "";
				if (selection_arr.length==1)
				{
					del_message  = "DELETE##"+delete_string+"##"+String.valueOf(Integer.parseInt(get_port())/2)+"##"+n.getNext();
				}
				else {
					del_message = "DELETE##" + delete_string + "##" + selection_arr[2]+"##"+n.getNext();
				}
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,del_message);
			}
			else if(hm.isEmpty()) {
				String del_message = "";
				if (selection_arr.length == 1) {
					del_message = "DELETE##" + delete_string + "##" + String.valueOf(Integer.parseInt(get_port()) / 2) + "##" + n.getNext();
				} else {
					del_message = "DELETE##" + delete_string + "##" + selection_arr[2] + "##" + n.getNext();
				}
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, del_message);
			}else {
				String del_message = "";
				del_message = "DELETE##"+delete_string+"##"+ String.valueOf(Integer.parseInt(get_port()) / 2);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, del_message);
				while(delete){
				}

			}
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {

		while (recovery);

		try {
			String port = String.valueOf(Integer.parseInt(get_port())/2);
			String key_hash = genHash(values.get("key").toString());

			String rightPartition = getTheRightPartition(key_hash);
			String[] succ = get_successor(rightPartition);
			if(port.equals(rightPartition)){
				hm.put(values.get("key").toString(), values.get("value").toString());
				Log.d(TAG,"Inserted "+values.get("key").toString()+" Hash Value: "+key_hash);
			}
			else{
				String insert_msg = "INSERT##"+rightPartition+"##"+values.get("key").toString()+"##"+values.get("value").toString();
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,insert_msg);
			}

			String insert_msg1 = "INSERT_REP##"+succ[0]+"##"+values.get("key").toString()+"##"+values.get("value").toString();
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,insert_msg1);
			String insert_msg2 = "INSERT_REP##"+succ[1]+"##"+values.get("key").toString()+"##"+values.get("value").toString();
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,insert_msg2);
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "Exception in insert");
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean onCreate() {
		try{
			ServerSocket serverSocket = new ServerSocket(ServerPort);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		}catch(Exception e){
			Log.e(TAG, "Exception in onCreate");
			e.printStackTrace();
		}
		recovery = true;
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"JOIN");
		Log.d(TAG, "In OnCreate sending JOIN");
		while(recovery){
		}
		return false;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
		while (recovery);
		String[] selection_ars = selection.split("##");
		String port = String.valueOf(Integer.parseInt(get_port())/2);

		try{
			if(selection_ars[0].equals("@")){
				Log.d(TAG, "In Query with selection: "+selection);
				cursor = null;
				String[] str = new String[] {"key","value"};
				MatrixCursor cur = new MatrixCursor(str);

				for (Map.Entry<String, String> entry : hm.entrySet()) {
					String k = entry.getKey();
					String val = entry.getValue();
					String[] curadd = new String[] {k,val};
					cur.addRow(curadd);
					Log.d(TAG, "In Query @ returning: "+k+" "+val);
				}
				cursor = cur;
				cursor.moveToFirst();

				return cur;
			}
			else if(selection_ars[0].equals("*")){
				Log.d(TAG, "In Query with selection: "+selection);

				n.setOrigin(port);
				String message_to_send = "STAR##"+n.getOrigin();
				Log.d(TAG, "In query propagating STAR: "+message_to_send);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message_to_send);
				while (cursor==null){
				}
				cursor.moveToFirst();
				Log.d(TAG, "In Query returning: "+cursor.getString(0)+" "+cursor.getString(0));
				return cursor;

			}
			else{
				Log.d(TAG, "In Query last else with selection:"+selection);

				String rightPartition = getTheRightPartition(genHash(selection_ars[0]));
				String[] successors = get_successor(rightPartition);

				try{
					if((port.equals(successors[1])||port.equals(successors[0])||port.equals(rightPartition))&&hm.get(selection_ars[0])!=null){
						Log.d(TAG, "In Query with selection: "+selection);
						String value = hm.get(selection_ars[0]);
						String[] str = new String[] {"key","value"};
						MatrixCursor cur = new MatrixCursor(str);
						String[] curadd = new String[] {selection_ars[0],value};
						cur.addRow(curadd);
						cursor = cur;
						cursor.moveToFirst();
						Log.d(TAG, "In Query returning: "+cursor.getString(0)+" "+cursor.getString(1));
						return cursor;
					}
					else {
						Log.d(TAG, "In Query with selection: "+selection);

						String query_message1 = "QUERY##"+selection_ars[0]+"##"+port+"##"+rightPartition;
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,query_message1);
						while (cursor==null){
						}
						cursor.moveToFirst();
						Log.d(TAG, "In Query returning: "+cursor.getString(0)+" "+cursor.getString(1));
						return cursor;
					}
				}catch (Exception e){

				}


			}
		}catch (Exception e){
			Log.e(TAG, "Exception in query");
			e.printStackTrace();
		}
		finally {
			cursor = null;
		}
		return cursor;

	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}


	/*--------------Client/Server FUNCTIONS------------------*/

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try{
				while(true){
					Socket socket = serverSocket.accept();
					DataInputStream in = new DataInputStream(socket.getInputStream());
					String message_in = in.readUTF();
					//Log.d(TAG,"Message in server::::: "+message_in);
					String[] msgs = message_in.split("##");
					//String port = String.valueOf(Integer.parseInt(get_port())/2);
					if(msgs[0].equals("JOIN")){
						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						String recovery_port = msgs[1];
						ConcurrentHashMap<String, String> belongsTo = new ConcurrentHashMap<String, String>();
						synchronized (this){
						//Log.e(TAG, "Hashmap size: "+hm.size());
						//Log.e(TAG, "Received recovery request from port: "+recovery_port);
						for (Map.Entry<String, String> entry : hm.entrySet()) {
							String rightPartition = getTheRightPartition(genHash(entry.getKey()));
							String[] succ = get_successor(rightPartition);
							if(rightPartition.equals(recovery_port)||succ[0].equals(recovery_port)||succ[1].equals(recovery_port)){
								belongsTo.put(entry.getKey(), entry.getValue());
							}
						}
						}
						String HashMapString = hashMapToString(belongsTo);
						out.writeUTF(HashMapString);
						out.flush();

					}
					if(msgs[0].equals("INSERT")){
						synchronized (this){
							String val = hm.put(msgs[2], msgs[3]);
							//Log.d(TAG, "Insertion done in port" +port+" key: "+msgs[2]+" value: "+msgs[3]+" put returned: "+val);
							//Log.d(TAG, "Key: "+msgs[2]+" retrieved at port: "+port+" with val: "+hm.get(msgs[2]));
						}


					}
					else if(msgs[0].equals("INSERT_REP")){
						synchronized (this){
							String val = hm.put(msgs[2], msgs[3]);
							//Log.d(TAG, "Replica Insertion done in port" +port+" key: "+msgs[2]+" value: "+msgs[3]+" put returned: "+val);
							//Log.d(TAG, "Key: "+msgs[2]+" retrieved at port: "+port+" with val: "+hm.get(msgs[2]));
						}

					}
					else if(msgs[0].equals("QUERY")){
						Log.d(TAG, "In server with QUERY message "+message_in);
						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						synchronized (this){
							String value = hm.get(msgs[1]);
							out.writeUTF(value);
						}

						out.flush();
					}
					else if(msgs[0].equals("RESPONSE")){
						Log.d(TAG, "In Server with response: "+message_in);
						String[] selection_arr = message_in.split("##");
						String[] str = new String[] {"key","value"};
						MatrixCursor cur = new MatrixCursor(str);
						String[] curadd = new String[] {selection_arr[1],selection_arr[3]};
						cur.addRow(curadd);
						cursor = cur;
					}
					else if(msgs[0].equals("STAR")){
						Log.d(TAG, "In server with STAR message "+message_in);
						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						synchronized (this){
							String HashMapString = hashMapToString(hm);
							Log.d(TAG, "In server sending global query response "+HashMapString);
							out.writeUTF(HashMapString);
						}

						out.flush();
					}
					else if(msgs[0].equals("DELETE")){
						synchronized (this){
							String removed = hm.remove(msgs[1]);
							//Log.d(TAG, "Removed from port: "+port+" key: "+msgs[1]+" value: "+removed);
						}

					}
				}
			}
			catch(Exception e){
				Log.e(TAG, "Exception in Server");
				e.printStackTrace();
			}
			return null;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {
		@Override
		protected Void doInBackground(String... msgs) {
			try {
				String[] client_message = msgs[0].split("##");
				String[] remote_ports = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
				String port = String.valueOf(Integer.parseInt(get_port())/2);
				Log.d(TAG,"msg received in Client: "+msgs[0]);
				if(client_message[0].equals("JOIN")){
					hm = new ConcurrentHashMap<String, String>();
					//hm.clear();
					n.setData(hm);
					//String port = String.valueOf(Integer.parseInt(get_port())/2);
					n.setOrigin(port);
					//n.setNode_hash(genHash(port));
					String[] successors = get_successor(port);
					//n.setPrev(get_predecessor(port));
					n.setNext(successors[0]);
					long startTime = System.nanoTime();
					for (String remote_port : remote_ports) {
						try {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remote_port));
							DataInputStream in = new DataInputStream(socket.getInputStream());
							DataOutputStream out = new DataOutputStream(socket.getOutputStream());
							String msg_to_send = "JOIN##" + port;
							//Log.d(TAG, "Remote port: "+remote_port);
							out.writeUTF(msg_to_send);
							out.flush();
							String HashMapString = in.readUTF();
							//ConcurrentHashMap<String, String> belongsHere;
							//belongsHere = stringToHashMap(HashMapString);
							//Log.d(TAG, "Returned Hashmap size: "+belongsHere.size());
							synchronized (this){
								hm.putAll(stringToHashMap(HashMapString));
							}
						} catch (Exception e) {
							Log.e(TAG, "Exception in Client! Server not up!");
							e.printStackTrace();
						}
					}
					recovery = false;
					//Log.d(TAG, "Final Size after recovery: "+n.getData().size());
					long endTime = System.nanoTime();
					long duration = (endTime - startTime);
					Log.d(TAG, "Total time for recovery: "+duration);
				}
				else if(client_message[0].equals("INSERT")){
					String callport = client_message[1];
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(callport)*2);
					DataOutputStream out = new DataOutputStream(socket.getOutputStream());
					out.writeUTF(msgs[0]);
				}
				else if(client_message[0].equals("INSERT_REP")){
					String callport = client_message[1];
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(callport)*2);
					DataOutputStream out = new DataOutputStream(socket.getOutputStream());
					out.writeUTF(msgs[0]);
				}
				else if(client_message[0].equals("QUERY")){
					Log.d(TAG, "Query message in client"+ msgs[0]);
					String rightPartition = client_message[3];
					String[] succ = get_successor(rightPartition);
					String[] str = new String[] {"key","value"};
					MatrixCursor cur = new MatrixCursor(str);

					try{
						String q1 = client_message[0]+"##"+client_message[1]+"##"+client_message[2]+"##"+succ[1];
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succ[1])*2);
						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						DataInputStream in = new DataInputStream(socket.getInputStream());
						Log.d(TAG, "Sending Query message from client"+ q1);
						out.writeUTF(q1);
						String value = in.readUTF();
						Log.d(TAG, "Received query reply from server: "+value);
						String[] curadd = new String[] {client_message[1],value};
						cur.addRow(curadd);
						cursor = cur;
					}catch (IOException e){
						Log.e(TAG, "Client query message: succ1 dead");
						try{
							String q2 = client_message[0]+"##"+client_message[1]+"##"+client_message[2]+"##"+succ[0];
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succ[0])*2);
							DataOutputStream out = new DataOutputStream(socket.getOutputStream());
							DataInputStream in = new DataInputStream(socket.getInputStream());
							Log.d(TAG, "Sending Query message from client"+ q2);
							out.writeUTF(q2);
							String value = in.readUTF();
							Log.d(TAG, "Received query reply from server: "+value);
							String[] curadd = new String[] {client_message[1],value};
							cur.addRow(curadd);
							cursor = cur;
						}catch (IOException e1){
							Log.e(TAG, "Client query message: succ2 dead");
							try{
								String q3 = client_message[0]+"##"+client_message[1]+"##"+client_message[2]+"##"+rightPartition;
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(rightPartition)*2);
								DataOutputStream out = new DataOutputStream(socket.getOutputStream());
								DataInputStream in = new DataInputStream(socket.getInputStream());
								Log.d(TAG, "Sending Query message from client"+ q3);
								out.writeUTF(q3);
								String value = in.readUTF();
								Log.d(TAG, "Received query reply from server: "+value);
								String[] curadd = new String[] {client_message[1],value};
								cur.addRow(curadd);
								cursor = cur;
							}catch (IOException e2){
								Log.e(TAG, "Everyone's dead!");
							}
						}
					}
				}
				else if(client_message[0].equals("STAR")){
					HashMap<String, String> StarResult = new HashMap<String, String>();

					for (String remote_port : remote_ports) {
						try {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remote_port));
							DataInputStream in = new DataInputStream(socket.getInputStream());
							DataOutputStream out = new DataOutputStream(socket.getOutputStream());
							String msg_to_send = "STAR##" + client_message[1];
							Log.d(TAG, "Remote port: "+remote_port);
							out.writeUTF(msg_to_send);
							out.flush();
							String HashMapString = in.readUTF();
							Log.d(TAG, "Received HashMap String in client for Star Query: "+HashMapString);
							ConcurrentHashMap<String, String> singleResult = stringToHashMap(HashMapString);
							StarResult.putAll(singleResult);

						} catch (Exception e) {
							Log.e(TAG, "Exception in Client! Server not up!");
							e.printStackTrace();
						}
					}
					String[] str = new String[] {"key","value"};
					MatrixCursor cur = new MatrixCursor(str);

					for (Map.Entry<String, String> entry : StarResult.entrySet()) {
						String k = entry.getKey();
						String val = entry.getValue();
						String[] curadd = new String[] {k,val};
						cur.addRow(curadd);
					}
					cursor = cur;
				}
				else if(client_message[0].equals("DELETE")){
					String key = client_message[1];

					for (String remote_port : remote_ports) {
						try {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remote_port));
							DataOutputStream out = new DataOutputStream(socket.getOutputStream());
							String msg_to_send = "DELETE##" + key;
							out.writeUTF(msg_to_send);
							out.flush();
						}catch (Exception e){
							Log.e(TAG, "Exception in Client! Server not up!");
						}
					}
					delete = false;
				}
			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");


			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		}
	}





	/*--------------HELPER FUNCTIONS------------------*/


	public String[] get_successor(String port){

		String succ1 = "";
		String succ2 = "";

		if(port.equals("5562")){
			succ1 = "5556";
			succ2 = "5554";
		}
		else if(port.equals("5556")){
			succ1 = "5554";
			succ2 = "5558";
		}
		else if(port.equals("5554")){
			succ1 = "5558";
			succ2 = "5560";
		}
		else if(port.equals("5558")){
			succ1 = "5560";
			succ2 = "5562";
		}
		else if(port.equals("5560")){
			succ1 = "5562";
			succ2 = "5556";
		}
		return new String[]{succ1, succ2};
	}

	public String getTheRightPartition(String keyhash){
		String rightPartition = "";

		try{
			if(keyhash.compareTo(genHash("5560"))>0||keyhash.compareTo(genHash("5562"))<=0){
				rightPartition = "5562";
			}
			else if(keyhash.compareTo(genHash("5562"))>0&&keyhash.compareTo(genHash("5556"))<=0){
				rightPartition = "5556";
			}
			else if(keyhash.compareTo(genHash("5556"))>0&&keyhash.compareTo(genHash("5554"))<=0){
				rightPartition = "5554";
			}
			else if(keyhash.compareTo(genHash("5554"))>0&&keyhash.compareTo(genHash("5558"))<=0){
				rightPartition = "5558";
			}
			else if(keyhash.compareTo(genHash("5558"))>0&&keyhash.compareTo(genHash("5560"))<=0){
				rightPartition = "5560";
			}
		}catch (Exception e){
			Log.e(TAG, "Exception in getTheRightPart");
			e.printStackTrace();
		}
		return rightPartition;

	}

	public String get_predecessor(String port){
		String rightSuccessor="";
		if(port.equals("5562")){
			rightSuccessor = "5560";
		}
		else if(port.equals("5556")){
			rightSuccessor = "5562";
		}
		else if(port.equals("5554")){
			rightSuccessor = "5556";
		}
		else if(port.equals("5558")){
			rightSuccessor = "5554";
		}
		else if(port.equals("5560")){
			rightSuccessor = "5558";
		}
		return rightSuccessor;
	}

	private String hashMapToString(ConcurrentHashMap<String, String> hashMap){
		String hashMapStr = "";

		if(hashMap == null){
			return  hashMapStr;
		}
		for (Map.Entry<String, String> entry : hashMap.entrySet()) {
			hashMapStr += (entry.getKey()+DELIMITER+entry.getValue()+DELIMITER2);
		}
		return hashMapStr;
	}


	private ConcurrentHashMap<String, String> stringToHashMap(String strHashMap){
		ConcurrentHashMap<String, String> newHashMap = new ConcurrentHashMap<String, String>();
		if(strHashMap == null || strHashMap.length()==0){
			return  null;
		}
		String hashMapKeyValuePairs[] = strHashMap.split(DELIMITER2);
		for(int i = 0; i < hashMapKeyValuePairs.length; i++){
			if(hashMapKeyValuePairs[i]!=null && hashMapKeyValuePairs[i].trim().length()!=0 && !hashMapKeyValuePairs[i].equals("")) {
				String keyVal[] = hashMapKeyValuePairs[i].split(DELIMITER);
				String key = keyVal[0];
				String value = keyVal[1];
				newHashMap.put(key, value);
			}
		}
		return newHashMap;
	}

	public String get_port(){
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((parseInt(portStr) * 2));
		return myPort;
	}

	public String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

}