package hdfs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Laaaag on 02/12/2019.
 */
public class Chunks implements Serializable{


    private  String name ;

    private long taille ;

    private  int facteurDeRepetition ;

    private  List<DataNode> datanodes ;


    public Chunks(String name, long taille, int facteurDeRepetition ) {

        super();
        this.facteurDeRepetition = facteurDeRepetition;
        this.name = name;
        this.taille = taille;
        this.datanodes = new ArrayList<DataNode>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTaille() {
        return taille;
    }

    public void setTaille(long taille) {
        this.taille = taille;
    }

    public List<DataNode> getDatanodes() {
        return datanodes;
    }

    public void addDatanode(DataNode datanode) {
        this.datanodes.add(datanode);
    }
    public void setDatanodes(List<DataNode> datanodes) {
        this.datanodes = datanodes;
    }

    @Override
    public String toString() {
         String fichier = "Nom :"+name+" ; SIZE : "+taille+" ; les datanodes : " ;

        for (int i = 0 ; i < datanodes.size() ; i++)
            fichier = fichier +" : "+datanodes.get(i);

        fichier = fichier + " FIN";

        return fichier;
    }
}
