package com.mr.net;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.mr.io.Collector;

/**
 * Starts the master server and keeps listening on port 7077 for 
 * incoming client requests. The client has a specific format for 
 * the input to server. The first value in list has the operation 
 * type and other values have data.
 * 
 * @author Shakti
 * 
 */
@SuppressWarnings("rawtypes")
public class MasterServer<KEY extends Comparable<KEY>,VALUE> {

	static Logger logger = Logger.getLogger(MasterServer.class.getName());
	static boolean listeningSocket = true;
	static int pivotCount = 0;
	static int otherDataCount = 0;
	Collector<KEY, VALUE> pivots = new Collector<KEY, VALUE>();
	Collector<KEY, VALUE> globalPivots = new Collector<KEY, VALUE>();
	Map<Integer, List<KEY>> nodeData = new HashMap<Integer, List<KEY>>();
	
	/**
	 * Starts the master server on port 7077
	 * Creates threads for each listening socket accepted.
	 *  
	 * @throws IOException
	 */
	public void startServer() throws IOException {
		
		// 3. Start Server
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(7077);
			logger.info("Waiting for client on port " + serverSocket.getLocalPort() + "...");
			while(listeningSocket){
				Socket serviceSocket = serverSocket.accept();
				MasterMiniServer miniserver = new MasterMiniServer(serviceSocket, this);
				miniserver.start();
			}
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Could not listen on port: 7077");
			logger.error(e.getLocalizedMessage());
		}		
	}
	
	/**
	 * @return Pivots to be used by other classes  
	 */
	public Collector<KEY, VALUE> getPivots(){
		synchronized (pivots) {
			return this.pivots;
		}
	}
	
	/**
	 * @return global Pivots to be used by other classes 
	 */
	public Collector<KEY, VALUE> getGlobalPivots(){
		synchronized (globalPivots) {
			return this.globalPivots;
		}
	}
}



/**
 * Master MiniServer is started for each client socket that is accepted.
 * It has all the functions to deal with the client request.
 * There is a switch case which operates on the type of request from the client.
 *   
 * @author Shakti
 *  
 */
@SuppressWarnings({"rawtypes", "unchecked"}) 
class MasterMiniServer<KEY extends Comparable<KEY>,VALUE> extends Thread {
	static Logger logger = Logger.getLogger(MasterMiniServer.class.getName());
	private Socket serviceSocket;
	private MasterServer<KEY, VALUE> master = null;

	/**
	 * 
	 * @param socket  : client socket 
	 * @param masterS : points to masterServer object that started this thread
	 * @throws IOException
	 */
	public MasterMiniServer(Socket socket, MasterServer masterS) throws IOException {
		serviceSocket = socket;
		master = masterS;
	}

	/**
	 * Thread run method. Serves the client requests
	 * 
	 */
	public void run() {
		ObjectInputStream in = null;
		ObjectOutputStream out = null;
		try {
			logger.info("Just connected to " + serviceSocket.getRemoteSocketAddress());
			in = new ObjectInputStream(serviceSocket.getInputStream());
			out = new ObjectOutputStream(serviceSocket.getOutputStream());
			List<Object> clientData = (List<Object>)in.readObject();
			String op = (String)clientData.get(0);
			
			switch (op) {
			
			/**
			 * gives the files to clients after distributing evenly 
			 * 
			 */
			case "getFiles":
				List<String> files = App.filesList[((Integer)clientData.get(1))-1];
				out.writeObject(files);
				break;
				
			/**
			 * Gets the pivots from the clients and stores in a variable
			 * Also increments the count to keep track of pivot requests 
			 * When all requests are served, creates global pivots
			 * 
			 */
			case "pivot":
				Collector<KEY, VALUE> clientPivots = (Collector<KEY, VALUE>)clientData.get(1);
				master.getPivots().addAll(clientPivots.toList());
				logger.info("Master pivot = "+ master.getPivots());
				MasterServer.pivotCount++;
				out.writeObject(true);
				int p = App.ipmap.size(), rho = p/2;
				if(MasterServer.pivotCount == p) {
					logger.info("p = "+ p + " \n rho = " + rho);
					master.getPivots().sort();
					for (int i = 0 ; i < (p-1); i++) {
						master.getGlobalPivots().collect(master.getPivots().toList().get(((i+1)*p)+rho-1));
					}
					logger.info("global pivots (print only once)= "+ master.getGlobalPivots());
				}
				logger.info("pivot request complete.....");
				break;
				
			/**
			 * Sends the global pivots to the clients 
			 */
			case "globalpivots":
				logger.info("global pivot request from client .....");
				while(MasterServer.pivotCount != App.ipmap.size()) {
					Thread.sleep(1000);
				} 
				out.writeObject(master.getGlobalPivots());
				logger.info("globalpivots request complete.....");
				break;
				
			/**
			 * Checks if all the nodes are ready for reduce function
			 */
			case "dataready":
				MasterServer.otherDataCount++;
				while(MasterServer.otherDataCount != App.ipmap.size());
				logger.info("dataready request complete..... for = " + MasterServer.otherDataCount);
				break;
			
			/**
			 * All tasks from the nodes are finished
			 */
			case "finish":
				logger.info("finish called");
				MasterServer.pivotCount ++;
				while(MasterServer.pivotCount != 2*App.ipmap.size()); 
				serviceSocket.close();
				Thread.sleep(1000);
				MasterServer.listeningSocket = false;
				break;
				
			default:
				break;
			}
		}
		catch(IOException | ClassNotFoundException | InterruptedException e){ 
			logger.error(e.getLocalizedMessage());
		} 
	}
}