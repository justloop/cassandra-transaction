package cs4224.project.cassandra.dataloader;

public class Warehouse {
	public int w_id;
	
	public String w_name;
	public String w_street_1;
	public String w_street_2;
	public String w_city;
	public String w_state;
	public String w_zip;
	public float w_tax;
	public float w_ytd;
	
	public Warehouse(String[] items) {
		this.w_id = Integer.parseInt(items[0]);
		this.w_name = items[1];
		this.w_street_1 = items[2];
		this.w_street_2 = items[3];
		this.w_city = items[4];
		this.w_state = items[5];
		this.w_zip = items[6];
		this.w_tax = Float.parseFloat(items[7]);
		this.w_ytd = Float.parseFloat(items[8]);
	}
}
