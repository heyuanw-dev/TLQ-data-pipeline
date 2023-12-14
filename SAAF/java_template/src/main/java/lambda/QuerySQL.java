import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LambdaHandler {

    public int lambdaHandler(Map<String, Object> request, Context context) {
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        inspector.inspectAllDeltas();

        String user = "admin";
        String pwd = "abcd1234";
        //String host = "salesdb.cluster-ro-czh7jhcwdkic.us-east-2.rds.amazonaws.com";
        String dbname = "salesdb";

        String s3Bucket = (String) request.get("s3_bucket");
        String s3Infile = (String) request.get("s3_infile");

        int result;

        try {
            Connection cnx = DriverManager.getConnection("jdbc:mysql://" + host + ":3306/" + dbname, user, pwd);
            PreparedStatement preparedStatement = cnx.prepareStatement("USE " + dbname);
            preparedStatement.execute();

            // User-defined table name
            String tableName = "sales_data_" + getTotalRecords(s3Bucket, s3Infile);

            String createTableQuery = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                    "Region VARCHAR(500) NOT NULL, " +
                    "Country VARCHAR(500) NOT NULL, " +
                    "Item_type VARCHAR(500) NOT NULL, " +
                    "Sales_channel VARCHAR(500) NOT NULL, " +
                    "Order_priority VARCHAR(500) NOT NULL, " +
                    "Order_date DATE, " +
                    "Order_Id int NOT NULL, " +
                    "Ship_date DATE, " +
                    "Units_sold int, " +
                    "Unit_price numeric, " +
                    "Unit_cost numeric, " +
                    "Total_Revenue numeric, " +
                    "Total_cost numeric, " +
                    "Total_profit numeric, " +
                    "Processing_time VARCHAR(500), " +
                    "Gross_Margin numeric);";

            preparedStatement = cnx.prepareStatement(createTableQuery);
            preparedStatement.execute();

            // Delete all data if exists
            preparedStatement = cnx.prepareStatement("TRUNCATE TABLE " + tableName + " ;");
            preparedStatement.execute();

            // Read CSV file from S3 and convert to list of tuples
            List<String> lines = readS3File(s3Bucket, s3Infile);
            List<Object[]> tuplesList = new ArrayList<>();
            for (String line : lines) {
                String[] values = line.split(",");
                tuplesList.add(values);
            }

            // Insert data into MySQL table
            String insertQuery = "INSERT INTO " + tableName + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            preparedStatement = cnx.prepareStatement(insertQuery);
            for (Object[] tuple : tuplesList) {
                for (int i = 0; i < tuple.length; i++) {
                    preparedStatement.setObject(i + 1, tuple[i]);
                }
                preparedStatement.addBatch();
            }

            // Calculate load time
            long startTime = System.currentTimeMillis();
            preparedStatement.executeBatch();
            long finishTime = System.currentTimeMillis();

            long runtime = finishTime - startTime;
            System.out.println("Runtime is " + runtime + " ms");
            System.out.println("Number of rows inserted: " + preparedStatement.getUpdateCount());
            cnx.commit();

            System.out.println("Successfully loaded data into table");
            preparedStatement.close();
            cnx.close();

            result = inspector.finish();
            result.put("runtime", runtime + " ms");
            result.put("row_count", tuplesList.size());

        } catch (SQLException | IOException e) {
            e.printStackTrace();
            result = -1; // indicate an error
        }

        return result;
    }

    private int getTotalRecords(String s3Bucket, String s3Infile) throws IOException {
        // Code to get total records from S3 file
        // You can implement this based on your actual requirements
        return 0;
    }

    private List<String> readS3File(String s3Bucket, String s3Infile) throws IOException {
        // Code to read CSV file from S3
        // You can implement this based on your actual requirements
        return new ArrayList<>();
    }

    // Define the Context class according to your actual requirements
    private static class Context {
    }

    // Define the Inspector class according to your actual requirements
    private static class Inspector {
        public void inspectAll() {
            // Implementation
        }

        public void inspectAllDeltas() {
            // Implementation
        }

        public int finish() {
            // Implementation
            return 0;
        }
    }
}
