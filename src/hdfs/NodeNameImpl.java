package hdfs;

import formats.Format;
import formats.KV;
import formats.LineFormat;
import jdk.internal.util.xml.impl.Input;

import java.io.*;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * Created by EL ALAOUI on 02/12/2019
 */


public class NodeNameImpl extends UnicastRemoteObject implements NameNode{

    static private int port = 3000;
    private List<DataNode> dataNodes;
    private List<DataNode> daemons;

    private String metaDataPath = "../data/";

    static public String config_path = "..\\config\\namenode.properties";
//    static public String config_path = "../config/namenode.properties";

    protected NodeNameImpl() throws RemoteException {
        super();
        dataNodes = new ArrayList<DataNode>();
        daemons = new ArrayList<DataNode>();

    }
    public static void main(String args[]){

       Properties properties = new Properties();
        InputStream inputStream ;
        try{
            inputStream = new FileInputStream(config_path);
            properties.load(inputStream);

            port = Integer.parseInt(properties.getProperty("port"));

            NameNode nameNode = new NodeNameImpl();
            LocateRegistry.createRegistry(port);
            Naming.rebind("//localhost:"+port+"/nameNodeDaemon",nameNode);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void configurationNameNode (String path){

    }

    public MetaDataFichier GetMetadataFile(String nomFich) throws RemoteException{
        // TODO : add Format first

        // Creation de type Format ... et le Metadata

        Format lineformat = new LineFormat(nomFich) ;
        lineformat.open(Format.OpenMode.R);

        MetaDataFichier metaData = new MetaDataFichier();


        //  informations du fichier et les chunks

        KV kv = lineformat.read();
    // information du fichier
        String[] informations = kv.v.split(":");
        metaData.setNomFich(informations[0]);
        metaData.setTaille(Integer.parseInt(informations[1]));
        metaData.setFormat(Format.Type.valueOf(informations[2]));


        // informations du chunks

        List<Chunks> chunksList = new ArrayList<Chunks>();

        while ((kv = lineformat.read()) != null){
            String[] informationsChunk = kv.v.split(":");

            Chunks chunk =new Chunks(informationsChunk[0],Long.parseLong(informationsChunk[1]),Integer.parseInt(informationsChunk[2]));

            for(int i = 0   ; i < Integer.parseInt(informationsChunk[2]) ; i++){
                chunk.addDatanode(new DataNode((informationsChunk[3+2*i]),
                        Integer.parseInt(informationsChunk[4+2*i])));
            }

            System.out.println("chunk ::::::::" +chunk);
            chunksList.add(chunk);


        }
        lineformat.close();
        metaData.setChunks(chunksList);
        return metaData;



    }

    public void addMetaDataFichier(MetaDataFichier fich) throws RemoteException {
        // TODO : add Format first

        Format format  = new LineFormat(fich.getNomFich());
        format.open(Format.OpenMode.W);


        KV kv = new KV();

        // lire les information du fichier (MetaData à ajouter et les chunks )

        kv.v = fich.toString();
        format.write(kv);

        for (Chunks chunk : fich.getChunks()){
            kv.v = fich.toString();
            format.write(kv);

        }

        format.close();

    }

    public void supprimerMetaDataFichier(String nomFich) throws RemoteException{
        File file = new File(nomFich);
        file.delete();
    }

    public List<DataNode> listeDataNodes() throws RemoteException {
        return dataNodes;
    }

    public void addDataNode(DataNode inf) throws RemoteException {
        dataNodes.add(inf);
    }


}