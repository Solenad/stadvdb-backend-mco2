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

                //System.out.println(String.format("[%s] Status: %d | Time: %dms | id= %s | firstName= %s",
                //user, response.statusCode(), (end - start), id, firstName)); // can do "Body: %s", response.body
                // testing (remove later)
                System.out.println(String.format("[%s] Status: %d | Time: %dms | Body = %s",
                user, response.statusCode(), (end - start), response.body()));

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

                //System.out.println(String.format("[%s] Status: %d | Time: %dms | id= %s | firstName= %s",
                //user, response.statusCode(), (end - start), id, firstName)); // can do "Body: %s", response.body
                System.out.println(String.format("[%s] Status: %d | Time: %dms | body = %s",
                user, response.statusCode(), (end - start), response.body()));

            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

// Step 4: Global Failure Recovery
    // helper function for put reqs to backend
    private static HttpResponse<String> doGet(String url) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(10))
            .GET()
            .build();

        long start = System.currentTimeMillis();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        long end = System.currentTimeMillis();

        System.out.println(String.format(
            "GET %s\nStatus: %d\nTime: %dms\nBody: %s\n",
            url, response.statusCode(), (end - start), response.body()
        ));
        return response;
    }

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
    private static void masterFailTest() {
        String isolation = "READ COMMITTED";
        try {
            String encoded = isolation.replace(" ", "%20");
            String baseUrl = centralnode + "/users/" + userid;
            String normalUrl = baseUrl + "?isolation=" + encoded;

            // sync value so test is clean
            String bodyBase = "{\"firstName\":\"notchanged\"}";
            doPut(normalUrl, bodyBase);
            System.out.println("Initial value (check if firstname is notchanged):");
            printRow();

            System.out.println("Stop the master node (press enter when done");
            System.in.read();

            // Case 1: attempt update while master is down (error 500)
            System.out.println("\nCase #1: Write while master is down");
            String failUrl = normalUrl;
            // backend receives write req, and since master node is down it runs changeMasterNode on services
            String bodyFail = "{\"firstName\":\"shouldFail\"}";
            try {
                doPut(failUrl, bodyFail);
            } catch (Exception e) {
                System.out.println("Failure during write since master node is down: " + e.getMessage());
            }

            System.out.println("Press enter once theres a new master assigned.");
            System.in.read();

            // Case 2: retry update after failover (new master)
            System.out.println("Case #2: Retrying same write after failover (should SUCCEED)");
            String bodySuccess = "{\"firstName\":\"afterFailover\"}";
            doPut(normalUrl, bodySuccess);

            System.out.println("Row from central (Should be firstName: afterFailover): ");
            printRow();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    // Test case 3-4: Fragment fails, central commits
    private static void fragmentFailTest() {
        String isolation = "READ COMMITTED";

        try {
            String encoded = isolation.replace(" ", "%20");
            String baseUrl = centralnode + "/users/" + userid;
            String normalUrl = baseUrl + "?isolation=" + encoded;

            // sync value so test is reset
            String bodyBase = "{\"firstName\":\"notchanged\"}";
            doPut(normalUrl, bodyBase);
            System.out.println("Initial value (check if firstname is notchanged):");
            printRow();

            // Case 3: attempt update while fragment is down
            System.out.println("\nCase #3: Write while fragment is down");
            System.out.println("Stop one fragment node (in this case node 1 since userid is from 2006 (enter once done).");
            System.in.read();

            String bodyCentralOnly = "{\"firstName\":\"centralOnly\"}";
            System.out.println("Performing write while fragment is down (central should still commit):");
            doPut(normalUrl, bodyCentralOnly);

            // central / master node will have firstname as "centralOnly" while 
            // node 1 still is on "notchanged"
            System.out.println("Central row after write with fragment down:");
            printRow();

            // Case 4: retry update after failover (fragment recovered)
            System.out.println("\nCase #4: Fragment node recovers and catches up missed writes");
            System.out.println("Start the node 1 fragment again (enter after waiting for recovery interval): ");
            System.in.read();

            // Central row firstName should be "centralOnly"
            System.out.println("Central row: ");
            printRow();

            // Change year based on user used, output here should be "centralOnly" now instead of "notchanged"
            int year = 2006;
            String yearUrl = centralnode + "/users/year/" + year;
            System.out.println("Fragment firstName after recovery: ");
            doGet(yearUrl);

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
                    /* 
                    for (int r = 1; r <= 3; r++){
                        System.out.print("\nRun " + r);
                        concurentReadTest(level);
                        Thread.sleep(2000);
                    }
                    
                    // test case 2
                    
                    for (int r = 2; r <= 3; r++){
                        System.out.print("\nRun " + r);
                        readWriteTest(level);
                        Thread.sleep(2000);
                    }

                    // test case 3
                    
                    for (int r = 3; r <= 3; r++){
                        System.out.print("\nRun " + r);
                        concurentWriteTest(level);
                        Thread.sleep(3000);
                    }*/
                }
                System.out.println("\n===== Test Cases for Global Failure Recovery ====");
                masterFailTest();
                Thread.sleep(2000);

                fragmentFailTest();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}