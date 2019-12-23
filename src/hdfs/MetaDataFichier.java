package hdfs; /**
 * Created by Laaaag on 02/12/2019.
 */

import formats.Format;

import java.io.Serializable;
import java.util.List;

public class MetaDataFichier implements Serializable {


    private String nomFich ;

    private List<Chunks> chunks;

    private long taille ;

    private Format.Type format ;

    public MetaDataFichier() {
    }

    public MetaDataFichier(String nomFich , long taille , Format.Type format){



        super();
        this.format = format ;
        this.nomFich = nomFich ;
        this.taille = taille ;
    }

    public Format.Type getFormat() {
        return format;
    }

    public void setFormat(Format.Type format) {
        this.format = format;
    }

    public String getNomFich() {
        return nomFich;
    }

    public void setNomFich(String nomFich) {
        this.nomFich = nomFich;
    }

    public List<Chunks> getChunks() {
        return chunks;
    }

    public void setChunks(List<Chunks> chunks) {
        this.chunks = chunks;
    }

    public long getTaille() {
        return taille;
    }

    public void setTaille(long taille) {
        this.taille = taille;
    }


}
