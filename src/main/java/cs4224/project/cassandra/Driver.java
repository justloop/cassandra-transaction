package cs4224.project.cassandra;

import com.datastax.driver.core.*;
import cs4224.project.cassandra.models.Orderline;
import cs4224.project.cassandra.models.OrderlineCodec;
import cs4224.project.cassandra.transactions.*;

import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by gejun on 15/10/16.
 */
public class Driver {
    private static final String defaultKeyspace = "d8";

    public static String keyspace = defaultKeyspace;
    private static Cluster cluster;
    private static Session session;

    private static void init(String keyspace) {
        CodecRegistry codecRegistry = new CodecRegistry();
        cluster = Cluster.builder().addContactPoints("localhost").withCodecRegistry(codecRegistry).withSocketOptions(new SocketOptions()
                .setConnectTimeoutMillis(2000).setReadTimeoutMillis(2000)).build();
        UserType orderlineType = cluster.getMetadata().getKeyspace(keyspace).getUserType("Orderline");
        TypeCodec<UDTValue> orderlineTypeCodec = codecRegistry.codecFor(orderlineType);
        OrderlineCodec orderlineCodec = new OrderlineCodec(orderlineTypeCodec, Orderline.class);
        codecRegistry.register(orderlineCodec);

        System.out.println("Trying to connect...");
        session = cluster.connect(keyspace);
        System.out.println("Connected successfully...");
    }

    private static void ProcessNewOrder(Scanner sc, String[] tokens) {
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
        NewOrder.execute(session,w_id,d_id,c_id,n,ol_i_ids,supply_w_ids,ol_i_ids);
    }

    private static void ProcessPayment(Scanner sc, String[] tokens) {
        int c_w_id = Integer.parseInt(tokens[1]);
        int c_d_id = Integer.parseInt(tokens[2]);
        int c_id = Integer.parseInt(tokens[3]);
        double payment_amount = Double.parseDouble(tokens[4]);
        Payment.execute(session,c_w_id,c_d_id,c_id,payment_amount);
    }

    private static void ProcessDelivery(Scanner sc, String[] tokens) {
        int w_id = Integer.parseInt(tokens[1]);
        int carrier_id = Integer.parseInt(tokens[2]);
        Delivery.execute(session,w_id,carrier_id);
    }

    private static void ProcessOrderStatus(Scanner sc, String[] tokens) {
        int c_w_id = Integer.parseInt(tokens[1]);
        int c_d_id = Integer.parseInt(tokens[2]);
        int c_id = Integer.parseInt(tokens[3]);
        OrderStatus.execute(session,c_w_id,c_d_id,c_id);
    }

    private static void ProcessStockLevel(Scanner sc, String[] tokens) {
        int w_id = Integer.parseInt(tokens[1]);
        int d_id = Integer.parseInt(tokens[2]);
        int t = Integer.parseInt(tokens[3]);
        int l = Integer.parseInt(tokens[4]);
        StockLevel.execute(session,w_id,d_id,t,l);
    }

    private static void ProcessPopularItem(Scanner sc, String[] tokens) {
        int w_id = Integer.parseInt(tokens[1]);
        int d_id = Integer.parseInt(tokens[2]);
        int l = Integer.parseInt(tokens[3]);
        PopularItem.execute(session,w_id,d_id,l);
    }

    private static void ProcessTopBalance(Scanner sc, String[] tokens) {
        TopBalance.execute(session);
    }

    public static void main(String[] args) throws InterruptedException {
        if(args.length == 2) {
            keyspace = args[1];
            init(args[1]);
        } else {
            init(defaultKeyspace);
        }
        Scanner sc = new Scanner(System.in);
        int totalExe = 0;
        long lStartTime = System.currentTimeMillis();
        while(sc.hasNextLine()) {
            String input = sc.nextLine();
            String[] tokens = input.split(",");

            switch(tokens[0]) {
                case "N":
                    System.out.println("New Order Transaction chosen:" + input);
                    ProcessNewOrder(sc, tokens);
                    System.out.println("New Order Transaction succeed");
                    break;
                case "P":
                    System.out.println("Payment Transaction chosen:" + input);
                    ProcessPayment(sc, tokens);
                    System.out.println("Payment Transaction succeed");
                    break;
                case "D":
                    System.out.println("Delivery Transaction chosen:" + input);
                    ProcessDelivery(sc, tokens);
                    System.out.println("Delivery Transaction succeed");
                    break;
                case "O":
                    System.out.println("Order-Status Transaction chosen:" + input);
                    ProcessOrderStatus(sc, tokens);
                    System.out.println("Order-Status Transaction succeed");
                    break;
                case "S":
                    System.out.println("Stock-Level Transaction chosen:" + input);
                    ProcessStockLevel(sc, tokens);
                    System.out.println("Stock-Level Transaction succeed");
                    break;
                case "I":
                    System.out.println("Popular-Item Transaction chosen:" + input);
                    ProcessPopularItem(sc, tokens);
                    System.out.println("Popular-Item Transaction succeed");
                    break;
                case "T":
                    System.out.println("Top-Balance Transaction chosen:" + input);
                    ProcessTopBalance(sc, tokens);
                    System.out.println("Top-Balance Transaction succeed");
                    break;
                default:
                    System.out.println("This is not a valid option.");
            }
            totalExe++;
        }

        long lEndTime = System.currentTimeMillis();
        long difference = lEndTime - lStartTime;

        System.err.println("Total transactions:" + totalExe);
        System.err.println("Total time elapsed in sec:" + (difference/1000));
        System.err.println("Transaction throughput per sec:" + (totalExe * 1000 / difference));
        System.out.println("Start session close");
        session.close();
        cluster.close();
        System.out.println("Driver closed");
    }
}
