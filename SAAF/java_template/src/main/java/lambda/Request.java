package lambda;

/**
 *
 * @author Wes Lloyd
 */
public class Request {

    String sql;

    public String getSql() {
        return sql;
    }
    
    public String getNameALLCAPS() {
        return sql.toUpperCase();
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Request(String sql) {
        this.sql = sql;
    }

    public Request() {

    }
}