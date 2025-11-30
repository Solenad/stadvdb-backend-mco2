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
    private static final HttpClient client = HttpClient.newHttpClient();

// Step 3: Concurrency control and Consistency
    // Test case 1: Concurrent reading
    private static void concurentReadTest(String isolationLevel) throws InterruptedException {
        System.out.println("\nCase #1 Level: " + isolationLevel);
        CountDownLatch countdown = new CountDownLatch(1);

        // user a to node2
        Thread userA = new Thread(new readTask(
            node2 + "/users/" + userid,
            isolationLevel,
            countdown,
            "UserA"
        ));

        // user b to node3
        Thread userB = new Thread(new readTask(
            node3 + "/users/" + userid,
            isolationLevel,
            countdown,
            "UserB"
        ));

        userA.start();
        userB.start();

        countdown.countDown(); 

        userA.join();
        userB.join();

        centralNodeValue();
    }   

    // Test case 2: One writing, other reading
     private static void readWriteTest(String isolationLevel) throws InterruptedException {
        System.out.println("\nCase #2 Level: " + isolationLevel);
        CountDownLatch countdown = new CountDownLatch(1); // makes sure both users start at the same time

        Thread userA = new Thread(new updateTask(
            node2 + "/users/" + userid,
            "test2",
            isolationLevel,
            countdown,
            "UserA"
        ));

        Thread userB = new Thread(new readTask(
            node3 + "/users/" + userid,
            isolationLevel,
            countdown,
            "UserB"
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
        System.out.println("\nCase #3 Level: " + isolationLevel);
        CountDownLatch countdown = new CountDownLatch(1);

        Thread userA = new Thread(new updateTask(
            node2 + "/users/" + userid, 
            "test3A", 
            isolationLevel, 
            countdown,
            "UserA"
        )); 

        Thread userB = new Thread(new updateTask(
            node3 + "/users/" + userid, 
            "test3B", 
            isolationLevel, 
            countdown,
            "UserB"
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
        String user;

        public readTask(String baseurl, String isolation, CountDownLatch latch, String user) {
            this.baseurl = baseurl;
            this.isolation = isolation;
            this.latch = latch;
            this.user = user;
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

                String body = response.body();
                String firstName = "N/A";
                String id = "N/A";
                try {
                    id = body.split("\"id\":")[1].split(",")[0];
                    firstName = body.split("\"firstName\":\"")[1].split("\"")[0];
                } catch (Exception ignored) {}

                System.out.println(String.format("[%s] Status: %d | Time: %dms | id=%s | firstName=%s",
                user, response.statusCode(), (end - start), id, firstName)); // can do "Body: %s", response.body

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
        String user;

        public updateTask(String baseurl, String newName, String isolation, CountDownLatch latch, String user) {
            this.baseurl = baseurl;
            this.newName = newName;
            this.isolation = isolation;
            this.latch = latch;
            this.user = user;
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

                String body = response.body();
                String firstName = "N/A";
                String id = "N/A";
                try {
                    id = body.split("\"id\":")[1].split(",")[0];
                    firstName = body.split("\"firstName\":\"")[1].split("\"")[0];
                } catch (Exception ignored) {}

                System.out.println(String.format("[%s] Status: %d | Time: %dms | id=%s | firstName=%s",
                user, response.statusCode(), (end - start), id, firstName)); // can do "Body: %s", response.body

            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

// Step 4: Global Failure Recovery
    // helper function for put reqs to backend
    private static HttpResponse<String> doPut(String url, String jsonBody) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/json")
            .PUT(BodyPublishers.ofString(jsonBody))
            .build();

        long start = System.currentTimeMillis();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        long end = System.currentTimeMillis();

        System.out.println(String.format("PUT %s\nStatus: %d\nTime: %dms\nBody: %s\n",
        url, response.statusCode(), (end - start), response.body()));
        return response;
    }

    // helper function for post reqs to backend
    private static HttpResponse<String> doPost(String url) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(10))
            .POST(BodyPublishers.noBody())
            .build();
        long start = System.currentTimeMillis();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        long end = System.currentTimeMillis();

        System.out.println(String.format("POST %s\nStatus: %d\nTime: %dms\nBody: %s\n",
        url, response.statusCode(), (end - start), response.body()));
        return response;
    }

    // prints out the current user row
    private static void printRow() {
        try {
            String url = centralnode + "/users/" + userid;
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();
            long start = System.currentTimeMillis();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            long end = System.currentTimeMillis();

            System.out.println(String.format("GET %s\nStatus: %d\nTime: %dms\nBody: %s\n",
            url, response.statusCode(), (end - start), response.body()));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    // Test case 1-2: Central node fails, fragment nodes commit
    private static void centralFailTest() {
        String isolation = "READ COMMITTED";

        try {
            String encoded = isolation.replace(" ", "%20");

            // syncs values for both fragment and central before failure
            String baseUrl = centralnode + "/users/" + userid;
            String normalUrl = baseUrl + "?isolation=" + encoded;
            String bodyBase = "{\"firstName\":\"sync\"}";
            doPut(normalUrl, bodyBase);
            System.out.println("After normal update:"); // no failure
            printRow();

            // fragment commits, central fails
            System.out.println("Case #1: \n");
            String failUrl = baseUrl + "?isolation=" + encoded + "&mode=centralfail";
            String bodyFragOnly = "{\"firstName\":\"committed\"}";
            doPut(failUrl, bodyFragOnly);
            System.out.println("Update after central fail: ");
            printRow();

            // recover central from fragment (both first_names should match now)
            System.out.println("Case #2: \n");
            String recoverUrl = centralnode + "/users/recovery/central/" + userid;
            doPost(recoverUrl);
            System.out.println("After recoverCentral:");
            printRow();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    // Test case 1-2: Fragment fails, central commits
    private static void fragmentFailTest() {
        String isolation = "READ COMMITTED";

        try {
            String encoded = isolation.replace(" ", "%20");

            // syncs values for both fragment and central before failure
            String baseUrl = centralnode + "/users/" + userid;
            String normalUrl = baseUrl + "?isolation=" + encoded;
            String bodyBase = "{\"firstName\":\"sync\"}";
            doPut(normalUrl, bodyBase);
            System.out.println("After normal update:");
            printRow();

            // central commits, fragment fails
            System.out.println("Case #3: \n");
            String failUrl = baseUrl + "?isolation=" + encoded + "&mode=fragmentfail";
            String bodyCentralOnly = "{\"firstName\":\"committed\"}";
            doPut(failUrl, bodyCentralOnly);
            System.out.println("Update after fragment fail:");
            printRow();

            // recover fragment from central (both first_names should match now)
            System.out.println("Case #4: \n");
            String recoverUrl = centralnode + "/users/recovery/fragment/" + userid;
            doPost(recoverUrl);
            System.out.println("After recoverFragment (fragment should now match central):");
            printRow();

        } catch (Exception e) {
            e.printStackTrace();
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
            System.out.println("===== Test Cases for Concurrency Control ====");
                for (String level : isolationLevels){
                    System.out.println("Isolation Level: " + level);

                    // test case 1
                    for (int r = 1; r <= 3; r++){
                        System.out.print("\nRun " + r);
                        concurentReadTest(level);
                        Thread.sleep(2000);
                    }

                    // test case 2
                    for (int r = 1; r <= 3; r++){
                        System.out.print("\nRun " + r);
                        readWriteTest(level);
                        Thread.sleep(2000);
                    }

                    // test case 3
                    for (int r = 1; r <= 3; r++){
                        System.out.print("\nRun " + r);
                        concurentWriteTest(level);
                        Thread.sleep(3000);
                    }
                }
            
            for (int i = 1; i <= 3; i++){
                System.out.println("\n===== Test Cases for Global Failure Recovery ====");
                centralFailTest(); // test cases 1-2
                Thread.sleep(2000);

                fragmentFailTest();// test cases 3-4
                Thread.sleep(2000);
            } 

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}