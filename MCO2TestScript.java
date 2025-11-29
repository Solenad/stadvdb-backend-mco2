import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublishers;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

public class MCO2TestScript {
    private static final String centralnode = "http://localhost:3000/api";
    private static final String node2 = "http://localhost:3000/api";
    private static final String node3 = "http://localhost:3000/api";

    private static final int userid = 61240;

    // Test case 1: Concurrent reading
    private static void concurentReadTest(String isolationLevel) throws InterruptedException {
        System.out.println("Case #1 Level: " + isolationLevel);
        CountDownLatch countdown = new CountDownLatch(1);

        // user a to node2
        Thread userA = new Thread(new readTask(
            node2 + "/users/" + userid,
            isolationLevel,
            countdown
        ));

        // user b to node3
        Thread userB = new Thread(new readTask(
            node3 + "/users/" + userid,
            isolationLevel,
            countdown
        ));

        userA.start();
        userB.start();

        countdown.countDown(); 

        userA.join();
        userB.join();
    }   

    // Test case 2: One writing, other reading
     private static void readWriteTest(String isolationLevel) throws InterruptedException {
        System.out.println("Case #2 Level: " + isolationLevel);
        CountDownLatch countdown = new CountDownLatch(1); // makes sure both users start at the same time

        Thread userA = new Thread(new updateTask(
            node2 + "/users/" + userid,
            "test2",
            isolationLevel,
            countdown
        ));

        Thread userB = new Thread(new readTask(
            node3 + "/users/" + userid,
            isolationLevel,
            countdown
        ));

        userA.start();
        userB.start();

        countdown.countDown(); 

        userA.join();
        userB.join();
        centralNodeValue();
     }

    // Test case 3: Concurrent writing
    private static void concurentWriteTest(String isolationLevel) throws InterruptedException {
        System.out.println("Case #3 Level: " + isolationLevel);
        CountDownLatch countdown = new CountDownLatch(1);

        Thread userA = new Thread(new updateTask(
            node2 + "/users/" + userid, 
            "test3A", 
            isolationLevel, 
            countdown
        )); 

        Thread userB = new Thread(new updateTask(
            node3 + "/users/" + userid, 
            "test3B", 
            isolationLevel, 
            countdown
        ));

        userA.start();
        userB.start();
        
        countdown.countDown(); 

        userA.join();
        userB.join();

        centralNodeValue();
    }

    private static void centralNodeValue() {
        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(centralnode + "/users/" + userid))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("Central Node Data: " + response.body());

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    // simulates a user doing select sql statements     
    static class readTask implements Runnable{
        String baseurl;
        String isolation;
        CountDownLatch latch;

        public readTask(String baseurl, String isolation, CountDownLatch latch) {
            this.baseurl = baseurl;
            this.isolation = isolation;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                latch.await(); 
                String encoded = isolation.replace(" ", "%20");
                String url = baseurl + "?isolation=" + encoded;

                HttpClient client = HttpClient.newHttpClient();
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .GET()
                        .build();

                long start = System.currentTimeMillis();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                long end = System.currentTimeMillis();

                System.out.println(String.format("Status: %d | Time: %dms | Body: %s\n",
                    response.statusCode(), (end - start), response.body()));

            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    // simulates a user doing update sql statements  
    static class updateTask implements Runnable {
        String baseurl;
        String newName;
        String isolation;
        CountDownLatch latch;

        public updateTask(String baseurl, String newName, String isolation, CountDownLatch latch) {
            this.baseurl = baseurl;
            this.newName = newName;
            this.isolation = isolation;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                latch.await(); 
                String encoded = isolation.replace(" ", "%20");
                String url = String.format("%s?isolation=%s", baseurl, encoded);
                String jsonBody = String.format("{\"firstName\":\"%s\"}", newName);

                HttpClient client = HttpClient.newHttpClient();
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/json")
                        .PUT(BodyPublishers.ofString(jsonBody))
                        .build();

                long start = System.currentTimeMillis();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                long end = System.currentTimeMillis();
                System.out.println(String.format("Status: %d | Time: %dms | Body: %s\n", response.statusCode(), (end - start), response.body()));

            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }
    public static void main(String[] args) {
        try {
            String[] isolationLevels = {
                "READ UNCOMMITTED",
                "READ COMMITTED",
                "REPEATABLE READ",
                "SERIALIZABLE"
            };
            for (String level : isolationLevels){
                System.out.println("Isolation Level: " + level);

                // test case 1
                concurentReadTest(level);
                Thread.sleep(2000);

                // test case 2
                readWriteTest(level);
                Thread.sleep(2000);

                // test case 3
                concurentWriteTest(level);
                Thread.sleep(3000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}