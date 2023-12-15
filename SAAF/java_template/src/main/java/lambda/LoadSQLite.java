package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import lambda.Request;
import saaf.Inspector;
import saaf.Response;

import java.io.File;
import java.io.InputStream;
import java.sql.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.UUID;

public class LoadSQLite implements RequestHandler<Request, HashMap<String, Object>> {
    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        LambdaLogger logger = context.getLogger();
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        String bucketname = request.getBucketname();
        String filename = request.getFilename();

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        //get object file using source bucket and srcKey name
        S3Object s3Object = s3Client.getObject(new GetObjectRequest("test.bucket.462562f23.lynnyang", "test.csv"));
        //get content of the file
        InputStream objectData = s3Object.getObjectContent();
        //scanning data line by line
        String text = "";

        Scanner scanner = new Scanner(objectData);
        while (scanner.hasNext()) {
            text += scanner.nextLine();
        }
        scanner.close();

        String pwd = System.getProperty("user.dir");
        logger.log("pwd=" + pwd);

        logger.log("set pwd to tmp");
        setCurrentDirectory("/tmp");

        pwd = System.getProperty("user.dir");
        logger.log("pwd=" + pwd);

        try
        {
            // Connection string an in-memory SQLite DB
            //Connection con = DriverManager.getConnection("jdbc:sqlite:");

            // Connection string for a file-based SQlite DB
            Connection con = DriverManager.getConnection("jdbc:sqlite:/tmp/mytest.db");

            // Detect if the table 'mytable' exists in the database
            PreparedStatement ps = con.prepareStatement("SELECT name FROM sqlite_master WHERE type='table' AND name='mytable'");
            ResultSet rs = ps.executeQuery();
            if (!rs.next())
            {
                // 'mytable' does not exist, and should be created
                logger.log("trying to create table 'mytable'");
                ps = con.prepareStatement("CREATE TABLE mytable ( name text, col2 text, col3 text);");
                ps.execute();
            }
            rs.close();

            // Insert row into mytable
            // ps = con.prepareStatement("insert into mytable values('" + request.getName() + "','" +
            //         UUID.randomUUID().toString().substring(0,8) + "','" + UUID.randomUUID().toString().substring(0,4) + "');");
            ps.execute();

            // Query mytable to obtain full resultset
            ps = con.prepareStatement("select * from mytable;");
            rs = ps.executeQuery();

            // Load query results for [name] column into a Java Linked List
            // ignore [col2] and [col3]
            LinkedList<String> ll = new LinkedList<String>();
            while (rs.next())
            {
                logger.log("name=" + rs.getString("name"));
                ll.add(rs.getString("name"));
                logger.log("col2=" + rs.getString("col2"));
                logger.log("col3=" + rs.getString("col3"));
            }
            rs.close();
            con.close();

            // r.setNames(ll);

            // sleep to ensure that concurrent calls obtain separate Lambdas
            try
            {
                Thread.sleep(200);
            }
            catch (InterruptedException ie)
            {
                logger.log("interrupted while sleeping...");
            }
        }
        catch (SQLException sqle)
        {
            logger.log("DB ERROR:" + sqle.toString());
            sqle.printStackTrace();
        }

        Response response = new Response();
        inspector.consumeResponse(response);
        return inspector.finish();
    }

    public static boolean setCurrentDirectory(String directory_name)
    {
        boolean result = false;  // Boolean indicating whether directory was set
        File directory;       // Desired current working directory

        directory = new File(directory_name).getAbsoluteFile();
        if (directory.exists() || directory.mkdirs())
        {
            result = (System.setProperty("user.dir", directory.getAbsolutePath()) != null);
        }

        return result;
    }
}
