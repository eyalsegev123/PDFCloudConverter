package local.assignment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Manager{
    
    // Reads the input file and processes each line based on the operation (ToImage, ToHTML, ToText)
    public static void processInputFile(String inputFilePath) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(inputFilePath));
            String line;
            while ((line = reader.readLine()) != null) {
                // Split the line by tab
                String[] parts = line.split("\t");
                String operation = parts[0];
                String pdfUrl = parts[1];

                // Call the appropriate method based on the operation
                switch (operation) {
                    case "ToImage":
                        PDFUtils.ToImage(pdfUrl);
                        break;
                    case "ToHTML":
                        PDFUtils.ToHTML(pdfUrl);
                        break;
                    case "ToText":
                        PDFUtils.ToText(pdfUrl);
                        break;
                    default:
                        System.out.println("Unknown operation: " + operation);
                }
            }
            reader.close();
        }
        catch(IOException e) {
            System.out.println("PDF not found or Invalid"); 
            e.printStackTrace();         
        }
    }

}