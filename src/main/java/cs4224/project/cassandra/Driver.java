package cs4224.project.cassandra;

import com.datastax.driver.core.*;
import cs4224.project.cassandra.models.Orderline;
import cs4224.project.cassandra.models.OrderlineCodec;
import cs4224.project.cassandra.transactions.*;

import java.util.Scanner;
import java.util.concurrent.*;

/**
 * Created by gejun on 15/10/16.
 */
public class Driver {
    private static Semaphore semaphore = new Semaphore(1000);
    private static int numberOfThreads = 20;
    private static ThreadPoolExecutor executor = null;


    private static Cluster cluster;
    private static Session session;

    private static void init() {
        CodecRegistry codecRegistry = new CodecRegistry();
        cluster = Cluster.builder().addContactPoint("localhost").withCodecRegistry(codecRegistry).build();
        UserType orderlineType = cluster.getMetadata().getKeyspace("d8").getUserType("Orderline");
        TypeCodec<UDTValue> orderlineTypeCodec = codecRegistry.codecFor(orderlineType);
        OrderlineCodec orderlineCodec = new OrderlineCodec(orderlineTypeCodec, Orderline.class);
        codecRegistry.register(orderlineCodec);

        System.out.println("Trying to connect...");
        session = cluster.connect("d8");
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

        doSubmit(()->NewOrder.execute(session,w_id,d_id,c_id,n,ol_i_ids,supply_w_ids,ol_i_ids));
    }

    private static void ProcessPayment(Scanner sc, String[] tokens) {
        int c_w_id = Integer.parseInt(tokens[1]);
        int c_d_id = Integer.parseInt(tokens[2]);
        int c_id = Integer.parseInt(tokens[3]);
        double payment_amount = Double.parseDouble(tokens[4]);
        doSubmit(()->Payment.execute(session,c_w_id,c_d_id,c_id,payment_amount));
    }

    private static void ProcessDelivery(Scanner sc, String[] tokens) {
        int w_id = Integer.parseInt(tokens[1]);
        int carrier_id = Integer.parseInt(tokens[2]);
        doSubmit(()->Delivery.execute(session,w_id,carrier_id));
    }

    private static void ProcessOrderStatus(Scanner sc, String[] tokens) {
        int c_w_id = Integer.parseInt(tokens[1]);
        int c_d_id = Integer.parseInt(tokens[2]);
        int c_id = Integer.parseInt(tokens[3]);
        doSubmit(()->OrderStatus.execute(session,c_w_id,c_d_id,c_id));
    }

    private static void ProcessStockLevel(Scanner sc, String[] tokens) {
        int w_id = Integer.parseInt(tokens[1]);
        int d_id = Integer.parseInt(tokens[2]);
        int t = Integer.parseInt(tokens[3]);
        int l = Integer.parseInt(tokens[4]);
        doSubmit(()->StockLevel.execute(session,w_id,d_id,t,l));
    }

    private static void ProcessPopularItem(Scanner sc, String[] tokens) {
        int w_id = Integer.parseInt(tokens[1]);
        int d_id = Integer.parseInt(tokens[2]);
        int l = Integer.parseInt(tokens[3]);
        doSubmit(()->PopularItem.execute(session,w_id,d_id,l));
    }

    private static void ProcessTopBalance(Scanner sc, String[] tokens) {
        doSubmit(()->TopBalance.execute(session));
    }

    public static void doSubmit(Runnable r){
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executor.submit(r);
    }

    public static void main(String[] args) {
        executor = new ThreadPoolExecutor(5,10,60,TimeUnit.SECONDS,new LinkedBlockingQueue<>()){
            protected void beforeExecute(Runnable r, Throwable t) {
                semaphore.release();
            }
        };

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
        executor.shutdown();
        boolean isEnded = false;
        try {
            isEnded = executor.awaitTermination(20000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if(!isEnded) {
            System.out.println("Executor was not shut down after timeout, not al tasks have been executed");
            executor.shutdownNow();

        }

    }
}
