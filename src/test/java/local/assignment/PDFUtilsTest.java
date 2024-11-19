package local.assignment;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class PDFUtilsTest {

    private final String testPdfPath = "/workspaces/ubuntu/Assignment1 - DSP/Assignment 1.pdf";
    private final String outputImage = "outputIMG.png";
    private final String outputHtml = "outputHTML.html";
    private final String outputText = "outputTEXT.txt";

    @BeforeEach
    void setup() {
        // Ensure no leftover output files from previous tests
        deleteFile(outputImage);
        deleteFile(outputHtml);
        deleteFile(outputText);
    }

    @AfterEach
    void cleanup() {
        // Clean up generated files after each test
        deleteFile(outputImage);
        deleteFile(outputHtml);
        deleteFile(outputText);
    }

    private void deleteFile(String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    void testToImage() {
        // Act
        PDFUtils.ToImage(new File(testPdfPath).toURI().toString());

        // Assert
        File outputFile = new File(outputImage);
        assertTrue(outputFile.exists(), "Image file should be generated.");
    }

    @Test
    void testToHTML() {
        // Act
        PDFUtils.ToHTML(new File(testPdfPath).toURI().toString());

        // Assert
        File outputFile = new File(outputHtml);
        assertTrue(outputFile.exists(), "HTML file should be generated.");
    }

    @Test
    void testToText() {
        // Act
        try {
            PDFUtils.ToText(new File(testPdfPath).toURI().toString());
        } catch (IOException e) {
            fail("Exception occurred while converting PDF to text: " + e.getMessage());
        }

        // Assert
        File outputFile = new File(outputText);
        assertTrue(outputFile.exists(), "Text file should be generated.");
    }
}
