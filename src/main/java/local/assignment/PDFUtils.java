package local.assignment;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class PDFUtils {

    // Converts the first page of the PDF to an image (PNG format) and returns the file
    public static File ToImage(String pdfUrl, String outputFilePath) {
        if(isValidUrl(pdfUrl)){
            try {
                // Load the PDF document from the URL
                PDDocument document = PDDocument.load(new URL(pdfUrl).openStream());

                // Create a renderer for the PDF document
                PDFRenderer renderer = new PDFRenderer(document);

                // Render the first page (page index 0) as an image with 300 DPI
                BufferedImage image = renderer.renderImageWithDPI(0, 300);

                // Save the image as a PNG file
                File outputFile = new File(outputFilePath);
                ImageIO.write(image, "PNG", outputFile);

                // Close the document to free resources
                document.close();

                System.out.println("First page successfully converted to: " + outputFilePath);
                return outputFile;
            } catch (IOException e) {
                System.out.println("Failed to convert PDF to image: " + e.getMessage());
                e.printStackTrace();
                return null;
            }
        }
        return null;
        
    }

    // Converts the first page of the PDF to an HTML file and returns the file
    public static File ToHTML(String pdfUrl, String outputFilePath) {
        if(isValidUrl(pdfUrl)){
            try {
                // Load the PDF document from the URL
                PDDocument document = PDDocument.load(new URL(pdfUrl).openStream());

                // Extract text from the first page using PDFTextStripper
                PDFTextStripper stripper = new PDFTextStripper();
                stripper.setStartPage(1);
                stripper.setEndPage(1);
                String pageText = stripper.getText(document);

                // Convert the extracted text to simple HTML
                String htmlContent = "<html><body><pre>" + pageText + "</pre></body></html>";

                // Save the HTML content to a file
                File outputFile = new File(outputFilePath);
                FileWriter writer = new FileWriter(outputFile);
                writer.write(htmlContent);
                writer.close();

                // Close the document to free resources
                document.close();

                System.out.println("First page successfully converted to HTML: " + outputFilePath);
                return outputFile;
            } catch (IOException e) {
                System.out.println("Failed to convert PDF to HTML: " + e.getMessage());
                e.printStackTrace();
                return null;
            }
        }
        return null;
    }

    // Converts the first page of the PDF to a text file and returns the file
    public static File ToText(String pdfUrl, String outputFilePath) {
        if(isValidUrl(pdfUrl)){
            try {
                // Load the PDF document from the URL
                PDDocument document = PDDocument.load(new URL(pdfUrl).openStream());

                // Extract text from the first page using PDFTextStripper
                PDFTextStripper stripper = new PDFTextStripper();
                stripper.setStartPage(1);
                stripper.setEndPage(1);
                String pageText = stripper.getText(document);

                // Save the text to a file
                File outputFile = new File(outputFilePath);
                FileWriter writer = new FileWriter(outputFile);
                writer.write(pageText);
                writer.close();

                // Close the document to free resources
                document.close();

                System.out.println("First page successfully converted to text: " + outputFilePath);
                return outputFile;
            } catch (IOException e) {
                System.out.println("Failed to convert PDF to text: " + e.getMessage());
                e.printStackTrace();
                return null;
            }
        }
        return null;
    }

    public static boolean isValidUrl(String urlString) {
        try {
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("HEAD");
            int responseCode = connection.getResponseCode();
            return (responseCode == HttpURLConnection.HTTP_OK);
        } catch (Exception e) {
            System.out.println("Invalid URL: " + urlString);
            return false;
        }
    }
}

