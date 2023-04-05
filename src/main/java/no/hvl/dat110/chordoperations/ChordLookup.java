/**
 * 
 */
package no.hvl.dat110.chordoperations;

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import no.hvl.dat110.middleware.Message;
import no.hvl.dat110.middleware.Node;
import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.Util;

/**
 * @author tdoy
 *
 */
public class ChordLookup {

	private static final Logger logger = LogManager.getLogger(ChordLookup.class);
	private Node node;
	
	public ChordLookup(Node node) {
		this.node = node;
	}
	
	public NodeInterface findSuccessor(BigInteger key) throws RemoteException {
		NodeInterface nodeSuccessor = node.getSuccessor();
		
		NodeInterface stub = Util.getProcessStub(nodeSuccessor.getNodeName(), nodeSuccessor.getPort());
		
		if(stub != null) {
			if(Util.checkInterval(key, node.getNodeID().add(new BigInteger("1")), nodeSuccessor.getNodeID())) {
				return nodeSuccessor;
			} else {
				NodeInterface highest_pred = findHighestPredecessor(key);
				return highest_pred.findSuccessor(key);
			}
		}
				
		return null;					
	}
	/**
	 * This method makes a remote call. Invoked from a local client
	 * @param ID BigInteger
	 * @return
	 * @throws RemoteException
	 */
	private NodeInterface findHighestPredecessor(BigInteger ID) throws RemoteException {
			
		List<NodeInterface> fingertable = node.getFingerTable();
		
		for(int i = fingertable.size() - 1; i >= 0; i--) {
			NodeInterface finger = fingertable.get(i);
			NodeInterface fingerStub = Util.getProcessStub(finger.getNodeName(), finger.getPort());

			BigInteger nodeIdplus1 = node.getNodeID().add(new BigInteger("1"));
			BigInteger keyminus1 = ID.subtract(new BigInteger("1")); 
			
			if(Util.checkInterval(fingerStub.getNodeID(), nodeIdplus1, keyminus1)) {
				return (NodeInterface) fingerStub;
			}
		}
		
		return (NodeInterface) node;			
	}
	
	public void copyKeysFromSuccessor(NodeInterface succ) {
		
		Set<BigInteger> filekeys;
		try {
		
			if(succ.getNodeName().equals(node.getNodeName()))
				return;
			
			logger.info("copy file keys that are <= "+node.getNodeName()+" from successor "+ succ.getNodeName()+" to "+node.getNodeName());
			
			filekeys = new HashSet<>(succ.getNodeKeys());
			BigInteger nodeID = node.getNodeID();
			
			for(BigInteger fileID : filekeys) {

				if(fileID.compareTo(nodeID) <= 0) {
					logger.info("fileID="+fileID+" | nodeID= "+nodeID);
					node.addKey(fileID); 															
					Message msg = succ.getFilesMetadata().get(fileID);				
					node.saveFileContent(msg.getNameOfFile(), fileID, msg.getBytesOfFile(), msg.isPrimaryServer()); 			
					succ.removeKey(fileID); 	 																				
					succ.getFilesMetadata().remove(fileID); 																	
				}
			}
			
			logger.info("Finished copying file keys from successor "+ succ.getNodeName()+" to "+node.getNodeName());
		} catch (RemoteException e) {
			logger.error(e.getMessage());
		}
	}

	public void notify(NodeInterface pred_new) throws RemoteException {
		
		NodeInterface pred_old = node.getPredecessor();
		
		// if the predecessor is null accept the new predecessor
		if(pred_old == null) {
			node.setPredecessor(pred_new);		// accept the new predecessor
			return;
		}
		
		else if(pred_new.getNodeName().equals(node.getNodeName())) {
			node.setPredecessor(null);
			return;
		} else {
			BigInteger nodeID = node.getNodeID();
			BigInteger pred_oldID = pred_old.getNodeID();
			
			BigInteger pred_newID = pred_new.getNodeID();
			
			// check that pred_new is between pred_old and this node, accept pred_new as the new predecessor
			// check that ftsuccID is a member of the set {nodeID+1,...,ID-1}
			boolean cond = Util.checkInterval(pred_newID, pred_oldID.add(BigInteger.ONE), nodeID.add(BigInteger.ONE));
			if(cond) {		
				node.setPredecessor(pred_new);		// accept the new predecessor
			}	
		}		
	}

}
