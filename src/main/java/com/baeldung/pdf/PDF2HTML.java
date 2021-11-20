package com.baeldung.pdf;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.fit.pdfdom.PDFDomTree;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

public class PDF2HTML {

    private static final String PDF = "src/main/resources/pdf.pdf";
    private static final String HTML = "src/main/resources/html.html";

    public static void main(String[] args) {
        try {
            generateHTMLFromPDF(PDF);
        } catch (IOException | ParserConfigurationException e) {
            e.printStackTrace();
        }
    }

    private static void generateHTMLFromPDF(String filename) throws ParserConfigurationException, IOException {
        PDDocument pdf = PDDocument.load(new File(filename));
        PDFDomTree parser = new PDFDomTree();
        Writer output = new PrintWriter("src/output/pdf.html", "utf-8");
        parser.writeText(pdf, output);
        output.close();
        if (pdf != null) {
            pdf.close();
        }
    }

}
