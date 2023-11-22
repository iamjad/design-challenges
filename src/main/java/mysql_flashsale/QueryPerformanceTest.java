package mysql_flashsale;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class QueryPerformanceTest {
    private static final Logger logger = LoggerFactory.getLogger(QueryPerformanceTest.class);

    public static void main(String[] args) {
        // Retrieve database connection details from environment variables
        String url = "jdbc:mysql://localhost:3306/myshop";
        String username = System.getenv("MYSQL_USERNAME");
        String password = System.getenv("MYSQL_PASSWORD");

        // Number of threads to execute in parallel
        int numberOfThreads = 10;

        // Configure HikariCP
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(numberOfThreads); // Set the maximum pool size

        // Create a HikariCP data source
        HikariDataSource dataSource = new HikariDataSource(config);

        // Create a thread pool
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
        long startTime = System.currentTimeMillis();
        // Execute the query using a thread pool
        for (int threadIndex = 0; threadIndex < 5000; threadIndex++) {
            executorService.execute(() -> {
                final int userId = (int) (Math.random() * 1000000);
                try {
                    executePickItemProcedure(dataSource, userId);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        // Shutdown the thread pool and wait for termination
        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error("Thread pool interrupted", e);
        }
        logger.info("Total execution time: " + (System.currentTimeMillis()-startTime) + " milliseconds");
        // Close the HikariCP data source
        dataSource.close();
    }

    private static int executePickItemProcedure(HikariDataSource dataSource, long userId) throws SQLException {
        // Stored procedure call
        String sql = "{CALL PickItem(?, ?)}"; // Assuming PickItem takes two parameters
        long startTime = System.currentTimeMillis();
        try (Connection connection = dataSource.getConnection();
             CallableStatement callableStatement = connection.prepareCall(sql)) {
            // Set the parameters
            callableStatement.setLong(1, userId);
            callableStatement.registerOutParameter(2, java.sql.Types.INTEGER);

            // Execute the stored procedure
            callableStatement.execute();
            logger.info("Total execution time: " + (System.currentTimeMillis()-startTime) + " milliseconds");
            // Get the result
            return callableStatement.getInt(2);
        }
    }

    private static void executeSampleQuery(HikariDataSource dataSource) {
        // Sample SQL query
        String sqlQuery = "SELECT * FROM item WHERE item_id = 1";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
             ResultSet resultSet = preparedStatement.executeQuery()) {

            // Process the result set as needed
            while (resultSet.next()) {
                int itemId = resultSet.getInt("item_id");
                String itemName = resultSet.getString("item_name");
                // Print or process the data as needed
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
