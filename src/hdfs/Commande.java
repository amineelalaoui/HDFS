package  hdfs;

import java.io.Serializable;
import formats.Format;
import formats.Format.Type;

public class Commande implements Serializable{

    private Id cmd;
    private String NomChunk;
    private Format.Type format;

    public static enum Id {Commande_WRITE,Commande_Read,Commande_Delete};

    public Commande(Id cmd, String NomChunk, Type format) {
        super();
        this.cmd = cmd;
        this.NomChunk = NomChunk;
        this.format = format;
    }




    public Id getCmd() {
        return cmd;
    }


    public void setCmd(Id cmd) {
        this.cmd = cmd;
    }


    public String getNomChunk() {
        return NomChunk;
    }


    public void setNomChunk(String NomChunk) {
        this.NomChunk = NomChunk;
    }


    public Format.Type getformat() {
        return format;
    }


    public void setformat(Format.Type format) {
        this.format = format;
    }


}
