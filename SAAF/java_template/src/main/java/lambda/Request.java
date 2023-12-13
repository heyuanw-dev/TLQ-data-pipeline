package lambda;

/**
 *
 * @author Jyoti Shankar
 */
public class Request {

    String bucketname;
    String filename;

    public String getBucketname() {
	    return bucketname;
    }

    public String getFilename() {
	    return filename;
    }

    public void setBucketname(String bucketname) {
	    this.bucketname =  bucketname;
    }

    public void setFilename(String filename) {
	    this.filename = filename;
    }

    public Request(String bucketname, String filename){
        this.bucketname = bucketname;
        this.filename = filename;
    }
    
    public Request() {

    }
}