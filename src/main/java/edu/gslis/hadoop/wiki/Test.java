
package edu.gslis.hadoop.wiki;

public class Test
{

    public static void main(String[] args) {
       double x= calculateEmim(2,1.0,4.0,5.0);     
       System.out.println(x);
    }
    
    public static double calculateEmim(double N, double nX1Y1, double nX1, double nY1)
    {
        
        //        | wordY | ~wordY |
        // -------|-------|--------|------
        //  wordX | nX1Y1 | nX1Y0  | nX1
        // ~wordX | nX0Y1 | nX0Y0  | nX0
        // -------|-------|--------|------
        //        |  nY1  |  nY0   | gt

        // Marginal and joint frequencies
        double nX0 = N - nX1;
        double nY0 = N - nY1;       
        double nX1Y0 = nX1 - nX1Y1;
        double nX0Y1 = nY1 - nX1Y1;
        double nX0Y0 = nX0 - nX0Y1;

        // Marginal probabilities (smoothed)
        double pX1 = (nX1 + 0.5)/(1+N);
        double pX0 = (nX0 + 0.5)/(1+N);         
        double pY1 = (nY1 + 0.5)/(1+N);
        double pY0 = (nY0 + 0.5)/(1+N);
        
        // Joint probabilities (smoothed)
        double pX1Y1 = (nX1Y1 + 0.25) / (1+N);
        double pX1Y0 = (nX1Y0 + 0.25) / (1+N);
        double pX0Y1 = (nX0Y1 + 0.25) / (1+N);
        double pX0Y0 = (nX0Y0 + 0.25) / (1+N);
        
        // 
        double emim =  
                pX1Y1 * log2(pX1Y1, pX1*pY1) + 
                pX1Y0 * log2(pX1Y0, pX1*pY0) +
                pX0Y1 * log2(pX0Y1, pX0*pY1) +
                pX0Y0 * log2(pX0Y0, pX0*pY0);
        
        return emim;
    }
    
    private static double log2(double num, double denom) {
        if (num == 0 || denom == 0)
            return 0;
        else
            return Math.log(num/denom)/Math.log(2);
    }
}