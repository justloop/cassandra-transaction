package cs4224.project.cassandra.transactions;

import java.util.Random;

public class TransactionUtils {
	private static Random rd = new Random();
	
	public static void randomSleep(){
		
		try {
			Thread.sleep(10 + rd.nextInt(20));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
