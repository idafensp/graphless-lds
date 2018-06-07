package oeg.upm.isantana.ldviz.utils;

/**
 * Ascii progress meter. On completion this will reset itself,
 * so it can be reused
 * <br /><br />
 * 100% ################################################## |
 */
public class ProgressBar {
    
    private int total;
    private int times;
    private int steps;
    private int count;
    private int perc;

    /**
     * initialize progress bar properties.
     */
    public ProgressBar(int t, int ts) {
    	this.total = t;
    	this.times = ts;
    	this.steps = t/ts +1;
    	this.perc = 0;
    	this.count = 0;
    }

    public void update() {

        if(count++%steps == 0)
          	  System.out.print(perc++ + "%...");
    }
    
    public void reset() {
    	this.perc = 0;
    	this.count = 0;
    }
    
}