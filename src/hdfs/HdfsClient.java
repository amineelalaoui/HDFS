/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Properties;
import java.util.*;

public class HdfsClient {
    static public String config_path = "D:/enseeiht/HDFS_FINAL/src/config/namenode.properties";
     static public String nameNodeN ;
    static public String nameNodeIP ;
    static public int nameNodePORT ;


    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }

    public static void loadConfig(String path){

        Properties properties = new Properties();
        InputStream inputStream;

        try {
            inputStream = new FileInputStream(config_path);
            properties.load(inputStream);
            nameNodeN = properties.getProperty("name");
            nameNodePORT = Integer.parseInt(properties.getProperty("port"));
            nameNodeIP = properties.getProperty("ip");


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void HdfsDelete(String hdfsFname) {

        try {
            loadConfig(config_path);
            Registry registry = LocateRegistry.getRegistry(nameNodeIP, nameNodePORT);
            NameNode nameNode = (NameNode) registry.lookup(nameNodeN);
            MetaDataFichier metadata = nameNode.GetMetadataFile(nameNodeN);

            List<Chunks> chunksList = metadata.getChunks();

            for (Chunks chunk : chunksList){
                List<DataNode> dataNodeList = chunk.getDatanodes();
                String ip = dataNodeList.get(0).getIp();
                int port = dataNodeList.get(0).getPort();


                Socket socket = new Socket(ip,port);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());

                Commande commande = new Commande(Commande.Id.Commande_Delete,chunk.getName()/*on doit avoir l*/,metadata.getFormat());

                objectOutputStream.writeObject(commande);
                objectOutputStream.close();

            }
            nameNode.supprimerMetaDataFichier(metadata.getNomFich());


        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, 
     int repFactor)  {

        //Writing Client
      try {
          System.out.println("Client Ecrit");

          loadConfig(config_path);
          System.out.println("rmi access : " + nameNodeIP + ":" + nameNodePORT);
          Registry registry = LocateRegistry.getRegistry(nameNodeIP, nameNodePORT);
          NameNode nameNode = (NameNode) registry.lookup(nameNodeN);
          List<DataNode> dataNodeList = nameNode.listeDataNodes();
          System.out.println(dataNodeList.get(0).getIp() + ":" + dataNodeList.get(0).getPort()+":" + dataNodeList.get(0).getName());

          File file = new File(localFSSourceFname);
          FileReader fileReader = new FileReader(file);
          BufferedReader bufferedReader = new BufferedReader(fileReader);
          //pour calculer le nombre de ligne
          int nombreLigne = 0 ;
          while (bufferedReader.readLine() != null) nombreLigne++;
          /// On cree le metadatafile

          MetaDataFichier metaDataFichier = new MetaDataFichier(localFSSourceFname,file.length(),fmt);
          List<Chunks> chunksList = new ArrayList<Chunks>();

          System.out.println(" Fichier existe ? : "+file.exists()+" "+localFSSourceFname+" "+file.length());
          System.out.println(dataNodeList.size()+" "+nombreLigne/dataNodeList.size()+ " " +nombreLigne%dataNodeList.size()+" " + nombreLigne);
          Format format = null ;
          switch(fmt)
          {
              case KV:
                   format = new KVFormat(file.getName());
                  break;
              case LINE:
                   format = new LineFormat();
                   break;
          }
          format.setFname(localFSSourceFname);
          format.open(Format.OpenMode.R);



          for (int i = 0 ; i < dataNodeList.size() ;i++ ){
              //Ici la commande de l'ecriture
              Commande commande = new Commande(Commande.Id.Commande_WRITE,localFSSourceFname+i,fmt);

              //envoie maint par les sockets
              List<Socket> socketListClient = new ArrayList<>();
              //Creation des outputstreams pour l'envoi

              List<ObjectOutputStream> objectOutputStreamList = new
                      ArrayList<>();
              for(int k = 0 ; k< repFactor ; k++){

                  int suivant = (i+k)%dataNodeList.size();
                  String ip =  dataNodeList.get(suivant).getIp();
                  int port = dataNodeList.get(suivant).getPort();
                  System.out.println(ip + ":" + port);
                  Socket socket = new Socket(ip,port);
                  socketListClient.add(socket);
                  objectOutputStreamList.add(new ObjectOutputStream(socketListClient.get(k).getOutputStream()));
                  System.out.println(commande.getCmd() + ":" + commande.getNomChunk() + ":" + commande.getformat());
                  objectOutputStreamList.get(k).writeObject(commande);
              }

              for(int k = 0 ; k < nombreLigne%dataNodeList.size() ; k++){
                  KV kv = format.read();

                  for(int j = 0 ; j < repFactor ; j++)
                      objectOutputStreamList.get(j).writeObject(kv);
              }

              // le reste à ajouter TODO

              //finalisation et ajout de metadatachunk et le dupliquer

              Chunks chunk = new Chunks(localFSSourceFname+i,nombreLigne%dataNodeList.size()+i,repFactor);
              for(int k = 0 ; k < repFactor ; k++){
                  int suivant = (i+k)%dataNodeList.size();
                  DataNode dataNode = dataNodeList.get(suivant);
                  chunk.addDatanode(dataNode);
                  objectOutputStreamList.get(k).close();
                  socketListClient.get(k).close();

              }


              chunksList.add(chunk);

          }

          metaDataFichier.setChunks(chunksList);
          nameNode.addMetaDataFichier(metaDataFichier);
          format.close();
      } catch (RemoteException e) {
          e.printStackTrace();
      } catch (NotBoundException e) {
          e.printStackTrace();
      } catch (FileNotFoundException e) {
          e.printStackTrace();
      } catch (IOException e) {
          e.printStackTrace();
      }


    }

    public static void HdfsRead(String hdfsFname, String localFSDestFname) {

        loadConfig(config_path);

        try {
            Registry registry = LocateRegistry.getRegistry(nameNodeIP,nameNodePORT);

            NameNode nameNode = (NameNode) registry.lookup(nameNodeN);
            System.out.println(hdfsFname);
            MetaDataFichier metaDataFichier = nameNode.GetMetadataFile(hdfsFname);


            List<Chunks> chunks = metaDataFichier.getChunks();

            for(Chunks chunk : chunks){


                List<DataNode> dataNodeList = chunk.getDatanodes();

                try {
                    Socket socket = new Socket(dataNodeList.get(0).getIp(),dataNodeList.get(0).getPort());

                    Commande commande = new Commande(Commande.Id.Commande_Read,localFSDestFname,Format.Type.KV);

                    ObjectOutputStream objectOutputStream =
                            new ObjectOutputStream(socket.getOutputStream());

                    objectOutputStream.writeObject(commande);

                    InputStream inputStream = socket.getInputStream();

                    ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);

                    while (objectInputStream.readObject()!=null){
                        System.out.println("Read encore "+objectInputStream.readObject().toString());
                    }
                    System.out.println("DONE");

                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }


            }

        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        }


    }

	
    public static void main(String[] args) {
        // java HdfsClient <read|write> <line|kv> <file>

        try {
            if (args.length<2) {usage(); return;}

            switch (args[0]) {
              case "read": HdfsRead(args[1],null); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write": 
                Format.Type fmt;
                if (args.length<3) {usage(); return;}
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {usage(); return;}
                HdfsWrite(fmt,args[2],1);
            }	
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }



}
