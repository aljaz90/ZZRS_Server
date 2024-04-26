package com.zzrs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
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
    double value;
    long travelTime;

    public MeterData(long id, long count, double value, long travelTime) {
        this.id = id;
        this.count = count;
        this.value = value;
        this.travelTime = travelTime;
    }
}

public class Main {
    public static final String USERNAME = "user";
    public static final String PASSWORD = "User123zzrs!";
    public static final int SERVER_PORT = 8080;
    private static final ScheduledExecutorService logExecutorService = Executors.newSingleThreadScheduledExecutor();
    private static final ScheduledExecutorService databaseExecutorService = Executors.newSingleThreadScheduledExecutor();
    private static Connection sqlConnection = null;
    public static List<MeterData> databaseQueue = new ArrayList<>();
    private static StringBuilder logQueue = new StringBuilder();

    static Calendar midnight = normalizeTimeDown(Calendar.getInstance());

    private static final int writeToDatabaseEvery = 20;
    private static final int writeToLogEvery = 20;

    public static void main(String[] args) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            sqlConnection = DriverManager.getConnection("jdbc:mysql://localhost/zzrs?user=" + USERNAME + "&password=" + PASSWORD);

            logExecutorService.scheduleAtFixedRate(Main::logToFile, writeToLogEvery, writeToLogEvery, TimeUnit.SECONDS);
            databaseExecutorService.scheduleAtFixedRate(Main::saveToDatabase, writeToDatabaseEvery, writeToDatabaseEvery, TimeUnit.SECONDS);

            HttpServer server = HttpServer.create(new InetSocketAddress(SERVER_PORT), 0);
            server.createContext("/report", new ZZRSHandler());
            server.setExecutor(null);
            server.start();
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }


    private static void handleMeterData(MeterData meterData) {
        databaseQueue.add(meterData);
    }

    private static void logToFile() {
        try {
            File directory = new File("./logs");
            if (!directory.exists()) {
                directory.mkdir();
            }

            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            File file = new File(directory, "log" + timestamp.toString().replaceAll(":", ".").replaceAll(" ", "T") + ".txt");

            BufferedWriter writer = new BufferedWriter(new FileWriter(file));

            String dataToWrite = logQueue.toString();
            logQueue.setLength(0);
            writer.write(dataToWrite);

            writer.close();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    private static void saveToDatabase() {
        try {
            PreparedStatement statement = sqlConnection.prepareStatement("INSERT INTO MeterData (id, count, value, travelTime) VALUES (?, ?, ?, ?)");

            List<MeterData> localDatabaseQueue = databaseQueue;
            databaseQueue = new ArrayList<>();

            int i = 0;
            while (!localDatabaseQueue.isEmpty()) {
                MeterData meterData = localDatabaseQueue.removeFirst();

                statement.setLong(1, meterData.id);
                statement.setLong(2, meterData.count);
                statement.setDouble(3, meterData.value);
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
        private Map<String, String> parseBody(String rawBody) {
            Map<String, String> toReturn = new HashMap<>();

            var params = rawBody.split("&");
            for (var param : params) {
                var pair = param.split("=");
                toReturn.put(pair[0], pair[1]);
            }

            return toReturn;
        }

        @Override
        public void handle(HttpExchange t) {
            try {
                Calendar calendar = Calendar.getInstance();
                long millisecondsFromMidnight = calendar.getTimeInMillis() - midnight.getTimeInMillis();

                InputStreamReader isr = new InputStreamReader(t.getRequestBody(), StandardCharsets.UTF_8);
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
                    long id = Long.parseLong(body.get("id"));
                    long count = Long.parseLong(body.get("count"));
                    double value = Double.parseDouble(body.get("value"));
                    long clientTimestamp = Long.parseLong(body.get("timestamp"));

                    long travelTime = Math.abs(millisecondsFromMidnight - clientTimestamp);
                    MeterData meterData = new MeterData(id, count, value, travelTime);

                    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                    StringBuilder stringBuilder = new StringBuilder()
                            .append("[R @ ")
                            .append(timestamp).append("] meter_id: ")
                            .append(meterData.id).append("; count: ")
                            .append(meterData.count).append("; value: ")
                            .append(meterData.value).append("; travel time: ")
                            .append(travelTime);

//                    System.out.println(stringBuilder);
                    logQueue.append(stringBuilder).append(System.lineSeparator());
                    handleMeterData(meterData);
                }
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
    }
}