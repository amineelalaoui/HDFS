package hdfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Created by Laaaag on 02/12/2019.
 */
public interface NameNode extends Remote {

    public MetaDataFichier GetMetadataFile(String nomFich) throws RemoteException;

    public void addMetaDataFichier(MetaDataFichier fich) throws RemoteException ;

    public void supprimerMetaDataFichier(String nomFich) throws RemoteException ;

    public List<DataNode> listeDataNodes() throws RemoteException ;

    public void addDataNode(DataNode inf) throws RemoteException ;




}
