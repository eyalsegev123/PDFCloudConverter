package local.assignment;

import org.junit.jupiter.api.Test;  // Import JUnit 5
import static org.junit.jupiter.api.Assertions.*;  // Import JUnit 5 assertions

import java.io.File;
import java.io.IOException;

public class PDFUtilsTest {

    // Test for converting PDF to image
    @Test
    public void testToImage() {
        try {
            // Provide a URL or path to a valid PDF file for testing
            String pdfUrl = "https://example.com/sample.pdf";  // Replace with actual PDF URL
            PDFUtils.ToImage(pdfUrl);

            // Check if the output image file exists
            File outputImageFile = new File("output.png");
            assertTrue(outputImageFile.exists(), "Output image file was not created");

            // Optional: Clean up after the test
            outputImageFile.delete();
        } catch (IOException e) {
            e.printStackTrace();
            fail("Exception occurred while converting PDF to image: " + e.getMessage());
        }
    }

    // Test for converting PDF to HTML
    @Test
    public void testToHTML() {
        try {
            // Provide a URL or path to a valid PDF file for testing
            String pdfUrl = "https://example.com/sample.pdf";  // Replace with actual PDF URL
            PDFUtils.ToHTML(pdfUrl);

            // Check if the output HTML file exists
            File outputHtmlFile = new File("output.html");
            assertTrue(outputHtmlFile.exists(), "Output HTML file was not created");

            // Optional: Clean up after the test
            outputHtmlFile.delete();
        } catch (IOException e) {
            e.printStackTrace();
            fail("Exception occurred while converting PDF to HTML: " + e.getMessage());
        }
    }

    // Test for converting PDF to text
    @Test
    public void testToText() {
        try {
            // Provide a URL or path to a valid PDF file for testing
            String pdfUrl = "https://example.com/sample.pdf";  // Replace with actual PDF URL
            PDFUtils.ToText(pdfUrl);

            // Check if the output text file exists
            File outputTextFile = new File("output.txt");
            assertTrue(outputTextFile.exists(), "Output text file was not created");

            // Optional: Clean up after the test
            outputTextFile.delete();
        } catch (IOException e) {
            e.printStackTrace();
            fail("Exception occurred while converting PDF to text: " + e.getMessage());
        }
    }

    // Test for processing a file with multiple operations
    @Test
    public void testProcessInputFile() {
        try {
            // Provide a path to a sample input file containing operations
            String inputFilePath = "path/to/input.txt";  // Replace with actual input file path
            PDFUtils.processInputFile(inputFilePath);

            // Check if the files are created based on the operations in the input file
            File outputImageFile = new File("output.png");
            File outputHtmlFile = new File("output.html");
            File outputTextFile = new File("output.txt");

            // Assert that the expected output files exist
            assertTrue(outputImageFile.exists(), "Output image file was not created");
            assertTrue(outputHtmlFile.exists(), "Output HTML file was not created");
            assertTrue(outputTextFile.exists(), "Output text file was not created");

            // Optional: Clean up after the test
            outputImageFile.delete();
            outputHtmlFile.delete();
            outputTextFile.delete();

        } catch (IOException e) {
            e.printStackTrace();
            fail("Exception occurred while processing input file: " + e.getMessage());
        }
    }
}
