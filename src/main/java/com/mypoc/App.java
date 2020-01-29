package com.nisum;

public class App {
    public static void main(String[] args) {
        try {
            new CsvToJson().processCsvToJson();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}