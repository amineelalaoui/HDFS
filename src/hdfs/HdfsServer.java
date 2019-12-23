package hdfs;

import formats.Format;
import formats.KV;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Properties;

public class HdfsServer implements Runnable {

    static public String ip;
    static public int port;
    static public String nameNodeIp;
    static int nameNodePort;
    static public String namenodeName;
    static public String path_config = "../config/namenode.properties";

    private Socket clientSocket;


    public void loadConfig(String path){
        Properties props = new Properties();

        try {
            InputStream input = new FileInputStream(path_config);
            props.load(input);
            nameNodeIp = props.getProperty("ip");
            nameNodePort = Integer.parseInt(props.getProperty("port"));
            namenodeName = props.getProperty("name");
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    private void deleteHDFS(String chunk){
        File file = new File(chunk);
        file.delete();

    }

    private void writeHDFS(Commande cmd, ObjectInputStream inputStream){
        Format format = Format.getFormatByType(cmd.getformat());
        format.setFname(cmd.getNomChunk());
        format.open(Format.OpenMode.W);
        KV enregistrement = null;
        boolean flag = false;
        try{
            do{
                enregistrement = (KV) inputStream.readObject();
                if(enregistrement==null) flag = false;
                else flag = true;
                if (flag) format.write(enregistrement);

            }while(flag);
        }catch(ClassNotFoundException e){
            e.printStackTrace();
        }catch(IOException ex){
            ex.printStackTrace();
        }
        format.close();
    }

    private void readHDFS(Commande cmd, ObjectOutputStream outputStream) {
        Format format = Format.getFormatByType(cmd.getformat());
        format.setFname(cmd.getNomChunk());
        format.open(Format.OpenMode.R);
        KV enregistrement = null;
        boolean flag = false;
        try{
            do{
                enregistrement = format.read();
                if(enregistrement==null) flag = false;
                else flag = true;
                if (flag) outputStream.writeObject(enregistrement);
            }while(flag);
            outputStream.writeObject(null);
        }catch(IOException ex){
            ex.printStackTrace();
        }
        format.close();
    }

    @Override
    public void run() {
        try {
            System.out.println("Connection Accepted");
            ObjectOutputStream outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            ObjectInputStream inputStream = new ObjectInputStream(clientSocket.getInputStream());


            Commande cmd = (Commande) inputStream.readObject();
            System.out.println(cmd);

            switch(cmd.getCmd()){
                case Commande_Read:
                    readHDFS(cmd,outputStream);
                    outputStream.close();
                    break;
                case Commande_WRITE:
                    writeHDFS(cmd,inputStream);
                    break;
                case Commande_Delete:
                    deleteHDFS(cmd.getNomChunk());
                    break;
            }


            inputStream.close();
            clientSocket.close();

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        ip = args[0];
        port = Integer.parseInt(args[1]);



        try {



            //register datanode
            loadConfig(path_config);
            DataNode dataNodeInfo= new DataNode(ip,port);
            Registry registry = LocateRegistry.getRegistry(nameNodeIp,nameNodePort);
            NameNode nameNode = (NameNode) registry.lookup(namenodeName);
            nameNode.addDataNode(dataNodeInfo);

            ServerSocket server = new ServerSocket(port);
            while(true)
            {
                System.out.println("attente");
                client = server.accept();
                System.out.println("accepted");
                HdfsServer hdfsServeur = new HdfsServer();
                hdfsServeur.start();
            }

        } catch (IOException | NotBoundException e) {
            e.printStackTrace();
        }
    }

}