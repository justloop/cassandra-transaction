package cs4224.project.cassandra.transactions;

import io.netty.util.internal.ThreadLocalRandom;

public class TransactionUtils {
	private static int lowerLimit = 50;
	private static int upperLimit = 100;
	
	public static void randomSleep(){
		
		try {
			Thread.sleep(ThreadLocalRandom.current().nextInt(lowerLimit, upperLimit));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
