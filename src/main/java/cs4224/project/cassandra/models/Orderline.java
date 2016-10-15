package cs4224.project.cassandra.models;

public class Orderline {

	private int ol_i_id;
	private double ol_amount;
	
	public Orderline(int a, double b){
		ol_i_id = a;
		ol_amount = b;
	}
	
	public int getId() {
		return ol_i_id;
	}
	
	public double getAmount() {
		return ol_amount;
	}
	
	@Override
	public String toString() {
		return String.format("<%d,%5.2f>", ol_i_id, ol_amount);
	}
}

