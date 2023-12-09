package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import saaf.Inspector;
import saaf.Response;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class Transform implements RequestHandler<Request, HashMap<String, Object>> {

    /**
     * Lambda Function Handler
     * 
     * @param request Request POJO with defined variables from Request.java
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        
        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAllDeltas();
        
        //****************START FUNCTION IMPLEMENTATION*************************
        
        String bucketname = request.getBucketname();
        String filename = request.getFilename();

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        // get object file using source bucket and srcKey name
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        // get content of the file
        InputStream objectData = s3Object.getObjectContent();

        StringWriter sw = new StringWriter();
        Scanner scanner = new Scanner(objectData);
   
        String nextline = scanner.nextLine();
        // Adding the columns
        nextline += ",Order Processing Time,Gross Margin\n";
        sw.append(nextline);

        ProcessCSV(scanner, sw, nextline);

        // Uploading processedcsv back to S3 bucket

        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
        InputStream is = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("text/csv");
        String filename2 = "output/TransformedSalesData.csv";
        s3Client.putObject(bucketname, filename2, is, meta);


        LambdaLogger logger = context.getLogger();
        logger.log("File uploaded" + bucketname + " filename:" + filename2);


        //Add custom key/value attribute to SAAF's output. (OPTIONAL)
        inspector.addAttribute("message", "Hello message");
        
        
        //Create and populate a separate response object for function output. (OPTIONAL)
        Response response = new Response();
        response.setValue("Bucket: " + bucketname + " filename:" + filename2 + " processed.");
        inspector.consumeResponse(response);
        
        //****************END FUNCTION IMPLEMENTATION***************************
        
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    private static StringWriter ProcessCSV(Scanner scanner, StringWriter sw, String nextline){

        Set<Long> uniqueOrderId = new HashSet<>();
        while (scanner.hasNext()) {
            nextline  = scanner.nextLine();
            String[] row = (nextline).split(",");
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

            String orderDate = row[5];
            String shipDate = row[7];
            int differenceDays = 0;

            // Removing duplicate data 
            long orderId = Long.parseLong(row[6]);

            if(uniqueOrderId.contains(orderId)) 
            	 continue;
            uniqueOrderId.add(orderId);

            try {
            // Parse string dates to Date objects
            Date startDate = sdf.parse(orderDate);
            Date endDate = sdf.parse(shipDate);

            // Calculate the difference in milliseconds
            long differenceMillis = endDate.getTime() - startDate.getTime();

            // Convert milliseconds to days
            differenceDays = (int)TimeUnit.DAYS.convert(differenceMillis, TimeUnit.MILLISECONDS);

            // Print the result
        } catch (ParseException e) {
            e.printStackTrace();
        }

        //updating order priority

            HashMap<String,String> orderPriorities =new HashMap<>();
            orderPriorities.put("L", "Low");
            orderPriorities.put("M", "Medium");
            orderPriorities.put("H", "High");
            orderPriorities.put("C", "Critical");
            
            
            String orderPriority = row[4];
            row[4] = orderPriorities.get(orderPriority);
            nextline = String.join(",", row);

        // Calculating gross margin

            double profit = Double.parseDouble(row[row.length -1]);
            double revenue = Double.parseDouble(row[row.length -3]);
            double grossMargin = profit / revenue;

            String lastcolumn = String.format(",%d,%.2f\n", differenceDays, grossMargin);
            nextline += lastcolumn;
            sw.append(nextline);
        }
        scanner.close();
        return sw;
    }
}
