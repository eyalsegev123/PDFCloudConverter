package local.assignment;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;

public class PDFUtils {

    // Converts the first page of the PDF to an image (PNG format)
    public static void ToImage(String pdfUrl) throws IOException {
        // Load the PDF document from URL
        PDDocument document = PDDocument.load(new URL(pdfUrl).openStream());

        // Get the first page of the document
        PDFRenderer renderer = new PDFRenderer(document);
        BufferedImage image = renderer.renderImageWithDPI(0, 300);  // Render with 300 DPI

        // Save the image as a PNG file
        File outputFile = new File("output.png");
        ImageIO.write(image, "PNG", outputFile);

        // Close the document
        document.close();
    }

    // Converts the first page of the PDF to an HTML file
    public static void ToHTML(String pdfUrl) throws IOException {
        // Load the PDF document from URL
        PDDocument document = PDDocument.load(new URL(pdfUrl).openStream());

        // Extract text from the first page using PDFTextStripper
        PDFTextStripper stripper = new PDFTextStripper();
        stripper.setStartPage(1);
        stripper.setEndPage(1);
        String pageText = stripper.getText(document);

        // Convert the extracted text to simple HTML
        String htmlContent = "<html><body><pre>" + pageText + "</pre></body></html>";

        // Save the HTML content to a file
        FileWriter writer = new FileWriter(new File("output.html"));
        writer.write(htmlContent);
        writer.close();

        // Close the document
        document.close();
    }

    // Converts the first page of the PDF to a text file
    public static void ToText(String pdfUrl) throws IOException {
        // Load the PDF document from URL
        PDDocument document = PDDocument.load(new URL(pdfUrl).openStream());

        // Extract text from the first page using PDFTextStripper
        PDFTextStripper stripper = new PDFTextStripper();
        stripper.setStartPage(1);
        stripper.setEndPage(1);
        String pageText = stripper.getText(document);

        // Save the text to a file
        FileWriter writer = new FileWriter(new File("output.txt"));
        writer.write(pageText);
        writer.close();

        // Close the document
        document.close();
    }

    // Reads the input file and processes each line based on the operation (ToImage, ToHTML, ToText)
    public static void processInputFile(String inputFilePath) throws IOException {
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
                    ToImage(pdfUrl);
                    break;
                case "ToHTML":
                    ToHTML(pdfUrl);
                    break;
                case "ToText":
                    ToText(pdfUrl);
                    break;
                default:
                    System.out.println("Unknown operation: " + operation);
            }
        }

        reader.close();
    }
}

