/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

import java.lang.annotation.Native;
import java.util.List;

/**
 *
 * @author wlloyd
 */
public class Response extends saaf.Response {
    
    //
    // User Defined Attributes
    //
    //
    // ADD getters and setters for custom attributes here.
    //

    // Return value
    private String value;
    public String getValue()
    {
        return value;
    }
    public void setValue(String value)
    {
        this.value = value;
    }

    public String results;
    public String getResults()
    {
        return this.results;
    }
    public void setResults(String results)
    {
        this.results = results;
    }
    public String getNamesString()
    {
        // StringBuilder sb = new StringBuilder();
        // for (String s : this.results)
        // {
        //     sb.append(s + "; ");
        // }
        // return sb.toString();
        return this.results;
    }

    public String mysqlversion;
    public String getMysqlversion()
    {
        return mysqlversion;
    }
    public void setMysqlversion(String mysqlversion)
    {
        this.mysqlversion = mysqlversion;
    }
    
    @Override
    public String toString()
    {
        return "value=" + this.getValue() + " " + this.getNamesString() + " " + this.getMysqlversion() + " " + super.toString(); 
    }

}
