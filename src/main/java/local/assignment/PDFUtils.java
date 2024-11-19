package local.assignment;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;

import javax.imageio.ImageIO;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;

public class PDFUtils {

    // Converts the first page of the PDF to an image (PNG format)
    public static void ToImage(String pdfUrl) {
        // Load the PDF document from URL
        try {
            PDDocument document = PDDocument.load(new URL(pdfUrl).openStream());

            // Get the first page of the document
            PDFRenderer renderer = new PDFRenderer(document);
            BufferedImage image = renderer.renderImageWithDPI(0, 300);  // Render with 300 DPI

            // Save the image as a PNG file
            File outputFile = new File("outputIMG.png");
            ImageIO.write(image, "PNG", outputFile);

            // Close the document
            document.close();
        }
        catch(IOException e){
            System.out.println("PDF not found or Invalid");
        }
        
        
    }

    // Converts the first page of the PDF to an HTML file
    public static void ToHTML(String pdfUrl) {
        // Load the PDF document from URL
        try {
            PDDocument document = PDDocument.load(new URL(pdfUrl).openStream());
            // Extract text from the first page using PDFTextStripper
            PDFTextStripper stripper = new PDFTextStripper();
            stripper.setStartPage(1);
            stripper.setEndPage(1);
            String pageText = stripper.getText(document);

            // Convert the extracted text to simple HTML
            String htmlContent = "<html><body><pre>" + pageText + "</pre></body></html>";

            // Save the HTML content to a file
            FileWriter writer = new FileWriter(new File("outputHTML.html"));
            writer.write(htmlContent);
            writer.close();

            // Close the document
            document.close();
        }
        catch(IOException e) {
            System.out.println("PDF not found or Invalid");
            e.printStackTrace();
        }
    }

    // Converts the first page of the PDF to a text file
    public static void ToText(String pdfUrl) throws IOException {
            
        try{
            // Load the PDF document from URL
            PDDocument document = PDDocument.load(new URL(pdfUrl).openStream());

            // Extract text from the first page using PDFTextStripper
            PDFTextStripper stripper = new PDFTextStripper();
            stripper.setStartPage(1);
            stripper.setEndPage(1);
            String pageText = stripper.getText(document);

            // Save the text to a file
            FileWriter writer = new FileWriter(new File("outputTEXT.txt"));
            writer.write(pageText);
            writer.close();

            // Close the document
            document.close();
        }
        catch(IOException e){
            System.out.println("PDF not found or Invalid");
            e.printStackTrace();
        }
    }

    
}
    

