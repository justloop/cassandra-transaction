package cs4224.project.cassandra.dataloader;

public class Customer {
	public int c_w_id;
	public int c_d_id;
	public int c_id;
	
	public String c_first;
	public String c_middle;
	public String c_last;
	public String c_street_1;
	public String c_street_2;
	public String c_city;
	public String c_state;
	public String c_zip;
	
	public String c_phone;
	public String c_since;
	public String c_credit;
	public float c_credit_ltm;
	public float c_discount;
	public float c_balance;
	
	public float c_ytd_payment;
	public int c_payment_cnt;
	public int c_delivery_cnt;
	public String c_data;
	
	public Customer(String[] items) {
		this.c_w_id = Integer.parseInt(items[0]);
		this.c_d_id = Integer.parseInt(items[1]);
		this.c_id = Integer.parseInt(items[2]);
		
		this.c_first = items[3];
		this.c_middle = items[4];
		this.c_last = items[5];
		
		this.c_street_1 = items[6];
		this.c_street_2 = items[7];
		this.c_city = items[8];
		this.c_state = items[9];
		this.c_zip = items[10];
		
	}
	

}
