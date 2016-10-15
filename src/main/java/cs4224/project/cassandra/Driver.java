package cs4224.project.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import cs4224.project.cassandra.transactions.*;

import java.util.Scanner;

/**
 * Created by gejun on 15/10/16.
 */
public class Driver {
    private static Cluster cluster;
    private static Session session;

    private static void init() {
        cluster = Cluster.builder().addContactPoint("127.0.0.1")
	    /*.withSocketOptions(
	      new SocketOptions()
	        .setConnectTimeoutMillis(10000000)
	        .setReadTimeoutMillis(10000000))*/
                .build();
        session = cluster.connect("D8");
    }

    private static boolean ProcessNewOrder(Scanner sc, String[] tokens) {
        int c_id = Integer.parseInt(tokens[1]);
        int w_id = Integer.parseInt(tokens[2]);
        int d_id = Integer.parseInt(tokens[3]);
        int n = Integer.parseInt(tokens[4]);
        int[] ol_i_ids = new int[n];
        int[] supply_w_ids = new int[n];
        int[] ol_quantities = new int[n];

        for (int i = 0; i < n; i++) {
            String line = sc.nextLine();
            String[] lineInput = line.split(",");
            ol_i_ids[i] = Integer.parseInt(lineInput[0]);
            supply_w_ids[i] = Integer.parseInt(lineInput[1]);
            ol_quantities[i] = Integer.parseInt(lineInput[2]);
        }
        return NewOrder.execute(session,w_id,d_id,c_id,n,ol_i_ids,supply_w_ids,ol_i_ids);
    }

    private static boolean ProcessPayment(Scanner sc, String[] tokens) {
        int c_w_id = Integer.parseInt(tokens[1]);
        int c_d_id = Integer.parseInt(tokens[2]);
        int c_id = Integer.parseInt(tokens[3]);
        double payment_amount = Double.parseDouble(tokens[4]);
        return Payment.execute(session,c_w_id,c_d_id,c_id,payment_amount);
    }

    private static boolean ProcessDelivery(Scanner sc, String[] tokens) {
        int w_id = Integer.parseInt(tokens[1]);
        int carrier_id = Integer.parseInt(tokens[2]);
        return Delivery.execute(session,w_id,carrier_id);
    }

    private static boolean ProcessOrderStatus(Scanner sc, String[] tokens) {
        int c_w_id = Integer.parseInt(tokens[1]);
        int c_d_id = Integer.parseInt(tokens[2]);
        int c_id = Integer.parseInt(tokens[3]);
        return OrderStatus.execute(session,c_w_id,c_d_id,c_id);
    }

    private static boolean ProcessStockLevel(Scanner sc, String[] tokens) {
        int w_id = Integer.parseInt(tokens[1]);
        int d_id = Integer.parseInt(tokens[2]);
        int t = Integer.parseInt(tokens[3]);
        int l = Integer.parseInt(tokens[4]);
        return StockLevel.execute(session,w_id,d_id,t,l);
    }

    private static boolean ProcessPopularItem(Scanner sc, String[] tokens) {
        int w_id = Integer.parseInt(tokens[1]);
        int d_id = Integer.parseInt(tokens[2]);
        int l = Integer.parseInt(tokens[3]);
        return PopularItem.execute(session,w_id,d_id,l);
    }

    private static boolean ProcessTopBalance(Scanner sc, String[] tokens) {
        return TopBalance.execute(session);
    }

    public static void main(String[] args) {

        Scanner sc = new Scanner(System.in);
        int totalExe = 0;
        long lStartTime = System.nanoTime();
        String input = null;
        while((input = sc.nextLine()) != null) {
            String[] tokens = input.split(",");

            switch(tokens[0]) {
                case "N":
                    System.out.println("New Order Transaction chosen");
                    ProcessNewOrder(sc, tokens);
                    break;
                case "P":
                    System.out.println("Payment Transaction chosen");
                    ProcessPayment(sc, tokens);
                    break;
                case "D":
                    System.out.println("Delivery Transaction chosen");
                    ProcessDelivery(sc, tokens);
                    break;
                case "O":
                    System.out.println("Order-Status Transaction chosen");
                    ProcessOrderStatus(sc, tokens);
                    break;
                case "S":
                    System.out.println("Stock-Level Transaction chosen");
                    ProcessStockLevel(sc, tokens);
                    break;
                case "I":
                    System.out.println("Popular-Item Transaction chosen");
                    ProcessPopularItem(sc, tokens);
                    break;
                case "T":
                    System.out.println("Top-Balance Transaction chosen");
                    ProcessTopBalance(sc, tokens);
                    break;
                default:
                    System.out.println("This is not a valid option.");

                    return;
            }
            totalExe++;
        }

        long lEndTime = System.nanoTime();
        long difference = lEndTime - lStartTime;


        System.err.println("Total transactions: " + totalExe);
        System.err.println("Total time elapsed in sec: " + (difference/1000));
        System.err.println("Transaction throughput per sec: " + (totalExe * 1000 / difference));

    }
}
