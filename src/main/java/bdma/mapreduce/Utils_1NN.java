package bdma.mapreduce;

import java.util.ArrayList;

public class Utils_1NN {

	private static final String[] attributes = new String[] {
		"id",
		"diagnosis",
		"mradius",
		"mtexture",
		"mperimeter",
		"marea",
		"msmooth",
		"mcompact",
		"mconcavity",
		"mconc_p",
		"msymmetry",
		"mfractal",
		"sradius",
		"stexture",
		"sperimeter",
		"sarea",
		"ssmooth",
		"scompact",
		"sconcavity",
		"sconc_p",
		"ssymmetry",
		"sfractal",
		"lradius",
		"ltexture",
		"lperimeter",
		"larea",
		"lsmooth",
		"lcompact",
		"lconcavity",
		"lconc_p",
		"lsymmetry",
		"lfractal",
		"train"
	};
	
	public static String getAttribute(String[] row, String attribute) {
		for (int i = 0; i < attributes.length; i++) {
			if (attributes[i].equals(attribute)) {
				return row[i];
			}
		}
		return null;
	}
	
	public static ArrayList<String> getPredictors() {
		ArrayList<String> predictors = new ArrayList<String>();
		for (int i = 0; i < attributes.length; i++) {
			if (!attributes[i].equals("id") && !attributes[i].equals("diagnosis") &&
					!attributes[i].equals("train")) {
				predictors.add(attributes[i]);
			}
		}
		return predictors;
	}
	
}
