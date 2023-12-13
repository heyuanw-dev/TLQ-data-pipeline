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

    private String results;
    public void setResults(String results) {
        this.results = results;
    }
    public String getResults() {
        return results;
    }

    @Override
    public String toString()
    {
        return "value=" + this.getValue() + " " + this.getResults() + " " + super.toString(); 
    }

}