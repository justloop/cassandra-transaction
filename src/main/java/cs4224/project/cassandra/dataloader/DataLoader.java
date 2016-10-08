package cs4224.project.cassandra.dataloader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataLoader {
	private static Logger logger = LoggerFactory.getLogger(DataLoader.class);
	
	public static void main(String[] args) {
		DataLoader loader = new DataLoader();
		loader.loadAll();
		logger.info("Size: {}", loader.warehouses.size());
 	}
	
	// All datas
	public Map<Integer, Warehouse> warehouses;
	
	public DataLoader() {
		this.warehouses = new HashMap<>();
	}
	
	public void loadAll() {
		this.loadWarehouse("D8-data/warehouse.csv");
	}
	
	public void loadWarehouse(String file) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			
			// Read all the bets
			String record = null;
			while((record = br.readLine()) != null) {
				Warehouse data = new Warehouse(record.split(","));
				this.warehouses.put(data.w_id, data);
			}
		} catch (Exception ex) {
			logger.error("Fail to load warehouse data", ex);
		}
	}
	

}
