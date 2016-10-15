package cs4224.project.cassandra.transactions;

import java.util.Random;

public class TransactionUtils {
	private static Random rd = new Random();
	
	public static void randomSleep(){
		
		try {
			Thread.sleep(50 + rd.nextInt(50));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
