package com.mohe.spring.dto;

public class MbtiPreference {
    
    private String extroversion; // E or I
    private String sensing;      // S or N  
    private String thinking;     // T or F
    private String judging;      // J or P
    
    public MbtiPreference() {}
    
    public MbtiPreference(String extroversion, String sensing, String thinking, String judging) {
        this.extroversion = extroversion;
        this.sensing = sensing;
        this.thinking = thinking;
        this.judging = judging;
    }
    
    public String getExtroversion() {
        return extroversion;
    }
    
    public void setExtroversion(String extroversion) {
        this.extroversion = extroversion;
    }
    
    public String getSensing() {
        return sensing;
    }
    
    public void setSensing(String sensing) {
        this.sensing = sensing;
    }
    
    public String getThinking() {
        return thinking;
    }
    
    public void setThinking(String thinking) {
        this.thinking = thinking;
    }
    
    public String getJudging() {
        return judging;
    }
    
    public void setJudging(String judging) {
        this.judging = judging;
    }
}