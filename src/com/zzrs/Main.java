package com.zzrs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class MeterData {
    long id;
    long timestep;
    long value;

    public MeterData(long id, long timestep, long value) {
        this.id = id;
        this.timestep = timestep;
        this.value = value;
    }
}

public class Main {
    public static final String USERNAME = "user";
    public static final String PASSWORD = "password";

    public static final int SERVER_PORT = 8080;


    private static ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private static Connection sqlConnection = null;

    public static Map<Integer, List<MeterData>> databaseQueue = new HashMap<>();
    public static List<Integer> timestepsSaved = new ArrayList<>();

    public static void main(String[] args)  {

        try {
            // Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            // sqlConnection = DriverManager.getConnection("jdbc:mysql://localhost/zzrs?user="+USERNAME+"&password="+PASSWORD);

            HttpServer server = HttpServer.create(new InetSocketAddress(SERVER_PORT), 0);
            server.createContext("/report", new ZZRSHandler());
            server.setExecutor(null);
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
         /*catch (IOException | SQLException exception) {
            exception.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }*/
    }


    // private static void handleMeterData(MeterData meterData) {
    //     if (databaseQueue.containsKey(meterData.timestep)) {
    //         databaseQueue.get(meterData.timestep).add(meterData);
    //     } else if (!timestepsSaved.contains(meterData.timestep)) {
    //         var list = new ArrayList<MeterData>();
    //         list.add(meterData);
    //         databaseQueue.put(meterData.timestep, list);

    //         executorService.schedule(() -> {
    //             saveTimestepToDatabase(meterData.timestep);
    //         }, 3, TimeUnit.SECONDS);
    //     }
    // }

    // private static void saveTimestepToDatabase(int timestep) {
    //     try {
    //         PreparedStatement statement = sqlConnection.prepareStatement("INSERT INTO MeterData (id, timestep, value) VALUES (?, ?, ?)");

    //         var list = databaseQueue.get(timestep);
    //         for (MeterData meterData : list) {
    //             statement.setInt(1, meterData.id);
    //             statement.setInt(2, meterData.timestep);
    //             statement.setInt(3, meterData.value);
    //             statement.addBatch();
    //         }

    //         statement.executeBatch();
    //     } catch (SQLException exception) {
    //         exception.printStackTrace();
    //     }
    // }

    static class ZZRSHandler implements HttpHandler {
        private Map<String, Long> parseBody(String rawBody) {
            Map<String, Long> toReturn = new HashMap<String, Long>();

            var params = rawBody.split("&");
            for (var param : params) {
                var pair = param.split("=");
                toReturn.put(pair[0], Long.valueOf(pair[1]));
            }

            return toReturn;
        }

        @Override
        public void handle(HttpExchange t) {
            try {
                InputStreamReader isr =  new InputStreamReader(t.getRequestBody(), StandardCharsets.UTF_8);
                BufferedReader br = new BufferedReader(isr);

                int b;
                StringBuilder buf = new StringBuilder(512);
                while ((b = br.read()) != -1) {
                    buf.append((char) b);
                }

                br.close();
                isr.close();

                String rawData = buf.toString();
                System.out.println("Got a request with raw body: " + rawData);

                String response = "OK";
                t.sendResponseHeaders(200, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();

                var body = this.parseBody(rawData);

                MeterData meterData = new MeterData(body.get("id"), body.get("timestep"), body.get("value"));
                System.out.println("[RECIEVED] meter_id:" + meterData.id + "; timestep: " + meterData.timestep + "; value: " + meterData.value);

                // handleMeterData(meterData);
            } catch(Exception exception) {
                exception.printStackTrace();
            }
        }
    }
}