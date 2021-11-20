import adapters.EC2Adapter;
import adapters.S3Adapter;
import adapters.SQSAdapter;
import org.apache.pdfbox.cos.COSDocument;
import org.apache.pdfbox.io.RandomAccessRead;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;
import org.fit.pdfdom.PDFDomTree;
import software.amazon.awssdk.services.sqs.model.Message;

import javax.xml.parsers.ParserConfigurationException;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;


public class WorekerMain {
    public static void main(String[] args) throws IOException, ParserConfigurationException {
        EC2Adapter ec2Adapter = new EC2Adapter();
        S3Adapter s3Adapter = new S3Adapter();
        SQSAdapter sqsAdapter = new SQSAdapter();
        List<Message> messages = sqsAdapter.retrieveMessage("queueUrl");
        for (Message m : messages) {
            String[] data = m.body().split(",");
            try {
                handleMessage(data[0], data[1]);
            } catch (Exception e) {
                //todo - If an exception occurs, then the worker should recover from it, send a message to the manager of the input message that caused the exception together with a short description of the exception, and continue working on the next message.
            }

        }
    }

    private static void saveFileFromUrlWithJavaIO(String fileName, String fileUrl)
            throws MalformedURLException, IOException {
        BufferedInputStream in = null;
        FileOutputStream fout = null;
        try {
            in = new BufferedInputStream(new URL(fileUrl).openStream());
            fout = new FileOutputStream(fileName);

            byte data[] = new byte[1024];
            int count;
            while ((count = in.read(data, 0, 1024)) != -1) {
                fout.write(data, 0, count);
            }

        } catch (MalformedURLException e) {
            //todo -If an exception occurs while performing an operation on a PDF file, or the PDF file is not available, then output line for this file will be: <operation>: input file <a short description of the exception>.
        } finally {
            if (in != null)
                in.close();
            if (fout != null)
                fout.close();
        }
    }

    private static void handleMessage(String url, String action) throws IOException, ParserConfigurationException {
        String dirName = "C:\\FileDownload";
        saveFileFromUrlWithJavaIO(dirName + "\\downloadPDFFIle.pdf", url);
        switch (action) {
            case "ToImage":
                toImage(dirName + "\\downloadPDFFIle.pdf");
                break;
            case "ToHTML":
                toHTML(dirName + "\\downloadPDFFIle.pdf");
                break;
            case "ToText":
                toText(dirName + "\\downloadPDFFIle.pdf");
                break;
            default:
                //toDo - what to do if no good action -//toDo - what to do if no good action - need to If an exception occurs while performing an operation on a PDF file, or the PDF file is not available, then output line for this file will be: <operation>: input file <a short description of the exception>.
                break;
        }
        //File f = new File(dirName + "\\downloadPDFFIle.pdf");//todo - delete the file
        //f.delete();

    }

    private static void toImage(String fileName) throws IOException, ParserConfigurationException {
        PDDocument document = PDDocument.load(new File(fileName));
        PDFRenderer pdfRenderer = new PDFRenderer(document);

        for (int page = 0; page < document.getNumberOfPages() && page < 1; ++page) {
            BufferedImage bim = pdfRenderer.renderImageWithDPI(
                    page, 300, ImageType.RGB);
            ImageIOUtil.writeImage(
                    bim, String.format("src/output/pdf-%d.%s", page + 1, "png"), 300);
        }
        document.close();
    }

    private static void toHTML(String fileName) throws IOException, ParserConfigurationException {
        PDDocument pdf = PDDocument.load(new File(fileName));
        PDPage page = pdf.getPage(0);
        PDDocument firstPage = new PDDocument();
        firstPage.addPage(page);
        Writer output = new PrintWriter("src/output/pdf.html", "utf-8");
        new PDFDomTree().writeText(firstPage, output);
        output.close();
    }

    private static void toText(String fileName) throws IOException, ParserConfigurationException {
        File f = new File(fileName);
        String parsedText;
        PDFParser parser = new PDFParser((RandomAccessRead) new RandomAccessFile(f, "r"));
        parser.parse();
        COSDocument cosDoc = parser.getDocument();
        PDFTextStripper pdfStripper = new PDFTextStripper();
        PDDocument pdDoc = new PDDocument(cosDoc);
        PDPage page = pdDoc.getPage(0);
        PDDocument firstPage = new PDDocument();
        firstPage.addPage(page);
        parsedText = pdfStripper.getText(firstPage);
        PrintWriter pw = new PrintWriter("src/output/pdf.txt");
        pw.print(parsedText);
        pw.close();
    }


}