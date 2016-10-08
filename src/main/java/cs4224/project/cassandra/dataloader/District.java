package cs4224.project.cassandra.dataloader;

public class District {
	public int d_w_id;
	public int d_id;
	
	public String d_name;
	public String d_street_1;
	public String d_street_2;
	public String d_city;
	public String d_state;
	public String d_zip;
	public float d_tax;
	public float d_ytd;
	public int d_next_o_id;
	
	public District(String[] items) {
		this.d_w_id = Integer.parseInt(items[0]);
		this.d_id = Integer.parseInt(items[1]);
		this.d_name = items[2];
		this.d_street_1 = items[3];
		this.d_street_2 = items[4];
		this.d_city = items[5];
		this.d_state = items[6];
		this.d_zip = items[7];
		this.d_tax = Float.parseFloat(items[8]);
		this.d_ytd = Float.parseFloat(items[9]);
		this.d_next_o_id = Integer.parseInt(items[10]);
	}

}
