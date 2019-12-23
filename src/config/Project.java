package config;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.HashMap;

import ihm.IHMLauncher;
import map.MapReduce;

public class Project {

	public static final String TMP_PATH = "/tmp/Hidoop/";
	public static final String PATH = "/home/tlompech/Hidoop/hidoop/";
	public static final String SRC_PATH = PATH+"src/";
	public static final String DATA_PATH = PATH+"data/";
	public static final String BIN_PATH = PATH+"bin/";
	public static final String BIN = "/home/tlompech/Hidoop/bin/";
	public static final int PORT = 3333;

	private static Class<?> interfaceMapReduce = null;
	private static int nbMap;
	private static MapReduce mapreduce;

	static public void initialiser(String nomImpl, String filename, int nbMap, boolean test, boolean local) {
		if (test) {
			IS_TEST = true;
			System.out.println("Test de Hidoop.");
	        nbMap = 3;
		} else {
			IS_TEST = false;
		}
		nomImpl = local ? nomImpl + "Local" : nomImpl;
		String strMaps = local? "" : " en utilisant "+nbMap+" maps.";
		System.out.println("Lancement de "+nomImpl+" sur le fichier "+filename+strMaps);
        MapReduce mr = loadImpl("map.MapReduce", nomImpl);
        // On peut lancer tout ce qu'il faut avant pour pas que ça rentre dans le timer
        // Appel de HDFS_Client pour slip le fichier filename
        // On récupérera plus tard la HashMap contenant le split effectué (dans le Job)
        String name = "newton";
        String cmd = "ssh "+name+" java --class-path "+BIN +" "+mr.getClass().getName()+" "+DATA_PATH+filename;
        //System.out.println(cmd);
        Process p = null;
        int end = -1;
        try {
			p = Runtime.getRuntime().exec(cmd);
			end = p.waitFor();
	    	BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
	    	String s = null;
	    	while ((s = in.readLine()) != null) {
	    		System.out.println(s);
	    	}
	        if (end == 0) {

	        } else {
	        	System.out.println("Echec de l'execution...");
	        }
        } catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
        //new IHMJob (); -> Dans le map dans ce cas là
        System.exit(0);
	}
	
	public static Class<?> getInterfaceMapReduce() {
		if (interfaceMapReduce == null) {
			return calculInterfaceMapReduce("MapReduce");
		} else {
			return interfaceMapReduce;
		}
	}

	private static Class<?> calculInterfaceMapReduce(String interfaceName) {
		Class<?> interf = null;
		String[] packages = (new File(SRC_PATH)).list();
    	// Recherche des classes implémentant l'interface trouvé
    	for (String pack : packages) {
			try {
	    		interf = Class.forName(pack + "."+interfaceName);
	    	} catch (ClassNotFoundException e) {
	    		//System.err.println ("Panic: ne trouve pas l'interface " + interfaceName+" dans : " +e);
	    	}
    	}
		// Cas où on n'a pas trouvé l'interface dans tous les packages
		if (interf == null) {
			System.err.println ("Impossible de trouver l'interface " + interfaceName);
			System.exit(1);
		}
		return interf;
	}

    private static MapReduce loadImpl (String interfName, String implName) {
    	MapReduce res = null;
        // Obtenir l'interface interfName
        Class<?> interf = Project.getInterfaceMapReduce();

        // Trouve la classe implName (ou interfName_implName)
        String[] packages = (new File(BIN_PATH)).list();
    	// Recherche des classes implémentant l'interface trouvé
        Class<?> implant = null;
    	for (String pack : packages) {
	        try {
	            implant = Class.forName(pack + "."+implName);
	        } catch (ClassNotFoundException e1) {
	            try {
	                implant = Class.forName(interfName+"_"+implName);
	            } catch (ClassNotFoundException e2) {
	                //System.err.println ("Impossible de trouver la classe "+implName+": "+e1);
	            }
	        }
    	}

    	// Vérifie qu'elle implante la bonne interface
        if (! interf.isAssignableFrom (implant)) {
            System.err.println ("La classe "+implant.getName()+" n'implante pas l'interface "+interf.getName()+".");
            return null;
        }
     
        // Crée une instance de cette classe
        try {
            Constructor cons = implant.getConstructor();
            Object[] initargs = {};
            res = (MapReduce) cons.newInstance (initargs);
        } catch (NoSuchMethodException e) {
            System.err.println ("Classe "+implant.getName()+": pas de constructeur adequat: "+e);
        } catch (InstantiationException e) {
            System.err.println ("Echec instation "+implant.getName()+": "+e);
        } catch (IllegalAccessException e) {
            System.err.println ("Echec instation "+implant.getName()+": "+e);
        } catch (java.lang.reflect.InvocationTargetException e) {
            System.err.println ("Echec instation "+implant.getName()+": "+e);
            if (e.getCause() != null) {
                System.err.println (" La cause est : " + e.getCause()
                  + " in " + (e.getCause().getStackTrace())[0]);
            }
        } catch (ClassCastException e) {
            System.err.println ("Echec instation "+implant.getName()+": n'est pas un "+interfName+": "+e);
        }
        return res;
    }

	/*
	 * Paramètre de test Hidoop sans HDFS
	 *  Les fichiers test_f1.txt (resp. test_f2.txt et test_f3.txt)
	 *  sont sur les disques locales de nymphe (resp. grove et eomer)
	 */
	public static boolean IS_TEST = true;
	
	private static String[] names = {"nymphe", "grove", "eomer"};
	private static String[] files = {"filesample_1.txt", "filesample_2.txt", "filesample_3.txt"};

	private static HashMap<String, String> genDic() {
		HashMap<String, String> dic = new HashMap<>();
		for (int i =0; i < files.length; i++) {
        	dic.put(names[i], TMP_PATH + files[i]);
        }
		return dic;
	}
	
	// Changer le nom de la méthode dic_test pour match avec HDFS
	public static HashMap<String,String> getSplit(String fname) {
		if (IS_TEST) {
			return genDic();
		} else {
			// Call to HDFS pour split fname si pas déjà fait
			return null;
		}
	}

	public static void main(String args[]) {
		new IHMLauncher(null);
	}
}
