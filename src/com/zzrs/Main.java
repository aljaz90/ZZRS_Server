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
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class MeterData {
    long id;
    long count;
    long value;
    long travelTime;

    public MeterData(long id, long count, long value, long travelTime) {
        this.id = id;
        this.count = count;
        this.value = value;
        this.travelTime = travelTime;
    }
}

public class Main {
    public static final String USERNAME = "user";
    public static final String PASSWORD = "password";
    public static final int SERVER_PORT = 8080;
    private static final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private static Connection sqlConnection = null;
    public static List<MeterData> databaseQueue = new ArrayList<>();

    static Calendar midnight = normalizeTimeDown(Calendar.getInstance());

    public static void main(String[] args)  {

        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            sqlConnection = DriverManager.getConnection("jdbc:mysql://localhost/zzrs?user="+USERNAME+"&password="+PASSWORD);

            executorService.scheduleAtFixedRate(Main::saveToDatabase, 60, 60, TimeUnit.SECONDS);

            HttpServer server = HttpServer.create(new InetSocketAddress(SERVER_PORT), 0);
            server.createContext("/report", new ZZRSHandler());
            server.setExecutor(null);
            server.start();
        } catch (IOException | SQLException | ClassNotFoundException | InstantiationException |
                 IllegalAccessException exception) {
            throw new RuntimeException(exception);
        }
    }


     private static void handleMeterData(MeterData meterData) {
         databaseQueue.add(meterData);
     }

     private static void saveToDatabase() {
         try {
             PreparedStatement statement = sqlConnection.prepareStatement("INSERT INTO MeterData (id, count, value, travelTime) VALUES (?, ?, ?, ?)");

             int i = 0;
             while(!databaseQueue.isEmpty()) {
                 MeterData meterData = databaseQueue.removeFirst();

                 statement.setLong(1, meterData.id);
                 statement.setLong(2, meterData.count);
                 statement.setLong(3, meterData.value);
                 statement.setLong(4, meterData.travelTime);
                 statement.addBatch();

                 if (i % 1000 == 0) {
                     statement.executeBatch();
                 }

                 i++;
             }

             statement.executeBatch();
         } catch (SQLException exception) {
             exception.printStackTrace();
         }
     }

    private static Calendar normalizeTimeDown(Calendar calendar) {
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        return calendar;
    }

    static class ZZRSHandler implements HttpHandler {
        private Map<String, Long> parseBody(String rawBody) {
            Map<String, Long> toReturn = new HashMap<String, Long>();

            var params = rawBody.split("&");
            for (var param : params) {
                var pair = param.split("=");
                long value = 0L;

                try {
                    value = Long.parseLong(pair[1]);
                } catch (Exception exception) {
                    exception.printStackTrace();
                }

                toReturn.put(pair[0], value);
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

                String response = "OK";
                t.sendResponseHeaders(200, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();

                var body = this.parseBody(rawData);

                if (body.containsKey("id") && body.containsKey("count") && body.containsKey("value") && body.containsKey("timestamp")) {
                    Calendar calendar = Calendar.getInstance();
                    long millisecondsFromMidnight =  calendar.getTimeInMillis() - midnight.getTimeInMillis();

                    long travelTime = millisecondsFromMidnight - body.get("timestamp");
                    MeterData meterData = new MeterData(body.get("id"), body.get("count"), body.get("value"), travelTime);
                    System.out.println("[RECIEVED] meter_id:" + meterData.id + "; count: " + meterData.count + "; value: " + meterData.value + "; travel time: " + travelTime);
                    handleMeterData(meterData);
                }
            } catch(Exception exception) {
                exception.printStackTrace();
            }
        }
    }
}