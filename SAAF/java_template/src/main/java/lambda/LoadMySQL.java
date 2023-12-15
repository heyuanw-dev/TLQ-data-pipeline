package lambda;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.core.ResponseInputStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import com.amazonaws.services.lambda.runtime.Context; 
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.protobuf.TextFormat.ParseException;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import saaf.Inspector;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;


public class LoadMySQL implements RequestHandler<HashMap<String, Object>, HashMap<String, Object>> {
    public HashMap<String, Object> handleRequest(HashMap<String, Object> request, Context context) {
        LambdaLogger logger = context.getLogger();

        Inspector inspector = new Inspector();
        inspector.inspectAll();
        Response response = new Response();
        try {
            String bukcetname = (String) request.get("bucketname");
            String filename = (String) request.get("filename");

            Properties properties = new Properties();
            properties.load(new FileInputStream("db.properties"));
            
            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            String dbname = "test";

            Connection con = DriverManager.getConnection(url,username,password);
            PreparedStatement preparedStatement = con.prepareStatement("USE " + dbname);
            preparedStatement.execute();

            // User-defined table name
            String tableName = "sales_data";

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

            preparedStatement = con.prepareStatement(createTableQuery);
            preparedStatement.execute();

            // Delete all data if exists
            preparedStatement = con.prepareStatement("TRUNCATE TABLE " + tableName + " ;");
            preparedStatement.execute();

            // Read CSV file from S3 and convert to list of tuples
            List<String> lines = readS3File(bukcetname, filename);
            List<Object[]> tuplesList = new ArrayList<>();
            for (String line : lines) {
                String[] values = line.split(",");
                tuplesList.add(values);
            }
            tuplesList.remove(0);

            // Insert data into MySQL table
            String insertQuery = "INSERT INTO " + tableName + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            preparedStatement = con.prepareStatement(insertQuery);
            for (Object[] tuple : tuplesList) {
                for (int i = 0; i < tuple.length; i++) {
                    if (i == 5 || i == 7) {
                        String dateString = (String) tuple[i]; // Cast to String if necessary
                        DateTimeFormatter originalFormat = DateTimeFormatter.ofPattern("M/d/yyyy");
                        // Parse the original date string
                        LocalDate date = LocalDate.parse(dateString, originalFormat);

                        // Define formatter for the desired format
                        DateTimeFormatter desiredFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");

                        // Format the date to the desired format
                        String formattedDate = date.format(desiredFormat);
                        preparedStatement.setString(i + 1, formattedDate);
                    } else {
                        preparedStatement.setObject(i + 1, tuple[i]);
                    }
                }
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();

            // logger.log(Arrays.toString(tuplesList.get(1)));
            // logger.log("Number of rows inserted: " + preparedStatement.getUpdateCount());

            response.setValue("Successfully loaded the database.");
            preparedStatement.close();
            con.close();

        } 
        catch (SQLException | IOException e) 
        {
            logger.log("Got an exception working with MySQL! ");
            e.printStackTrace();
            logger.log(e.getMessage());
        }

        inspector.consumeResponse(response);
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    private List<String> readS3File(String s3Bucket, String s3Infile) throws IOException {
        List<String> lines = new ArrayList<>();

        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_2) // Set your region
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        try (ResponseInputStream<GetObjectResponse> s3Object = s3.getObject(GetObjectRequest.builder()
                .bucket(s3Bucket)
                .key(s3Infile)
                .build())) {
            Reader reader = new InputStreamReader(s3Object);
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(reader);
            for (CSVRecord record : records) {
                StringBuilder line = new StringBuilder();
                record.forEach(e -> line.append(e).append(","));
                lines.add(line.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return lines;
    }

}