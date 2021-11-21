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

    public static final int MALFORMED_URL_EXCEPTION = 1;
    public static final int IO_Exception = 2;
    public static final int SUCCESS_TO_DOWONLOAD = 3;

    // todo - there should be 2 sqs queues : one for sending and one for receiving- the one for receiving is at line 34 and 56 all the rest is the sending sqs-queue;
    // todo - If a worker stops working unexpectedly before finishing its work on a message, then some other worker should be able to handle that message.
    public static void main(String[] args) throws IOException, ParserConfigurationException {
        EC2Adapter ec2Adapter = new EC2Adapter();
        S3Adapter s3Adapter = new S3Adapter();
        SQSAdapter sqsAdapter = new SQSAdapter();
        List<Message> messages = sqsAdapter.retrieveMessage("queueUrl"); //todo is that the queueurl?
        String fileDir = null;
        for (Message m : messages) {
            String[] data = m.body().split(",");
            try {
                fileDir = handleMessage(data[0], data[1], sqsAdapter, s3Adapter);
                if (fileDir == "no such commend") {
                    sqsAdapter.sendMessage("queueUrl", data[1] + "," + data[0] + ",do not support " + data[1]); //todo is that the queue-url?
                } else if (fileDir == "could not downlowd the file") {
                    continue;
                } else {
                    s3Adapter.putFileInBucketFromPath("<buckeName>", "<key>", fileDir);

                    sqsAdapter.sendMessage("queueUrl", data[1] + "," + data[0] + ",<s3URL>"); //todo - chancge the s3url and queuee-url
                    File f = new File(fileDir + "\\downloadPDFFIle.pdf");//todo - delete the file
                    f.delete();
                }

            } catch (Exception e) {
                sqsAdapter.sendMessage("queueUrl", data[1] + "," + data[0] + ",has failed duo to ParserConfigurationException or IOException"); //todo is that the queue-url?
            }
        }
        sqsAdapter.deleteMessage(messages, "<queueUrl>"); // todo i think that shuold be anther sqs
    }

    private static int saveFileFromUrlWithJavaIO(String fileName, String fileUrl)
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
            return MALFORMED_URL_EXCEPTION;

        } catch (IOException e) {
            return IO_Exception;

        } finally {
            if (in != null)
                in.close();
            if (fout != null)
                fout.close();
        }
        return SUCCESS_TO_DOWONLOAD;
    }

    private static String handleMessage(String url, String action, SQSAdapter sqsAdapter, S3Adapter s3Adapter) throws IOException, ParserConfigurationException {
        String dirName = "C:\\FileDownload";
        if (saveFileFromUrlWithJavaIO(dirName + "\\downloadPDFFIle.pdf", url) == MALFORMED_URL_EXCEPTION) {
            sqsAdapter.sendMessage("queueUrl", action + "," + url + ":the url is malformed"); //todo is that the queue-url?
            return "could not download the file";

        } else if (saveFileFromUrlWithJavaIO(dirName + "\\downloadPDFFIle.pdf", url) == IO_Exception) {
            sqsAdapter.sendMessage("queueUrl", action + "," + url + ":IOException has happened"); //todo is that the queueu-rl?
            return "could not download the file";
        }
        switch (action) {
            case "ToImage":
                dirName = toImage(dirName + "\\downloadPDFFIle.pdf");
                break;
            case "ToHTML":
                dirName = toHTML(dirName + "\\downloadPDFFIle.pdf");
                break;
            case "ToText":
                dirName = toText(dirName + "\\downloadPDFFIle.pdf");
                break;
            default:
                dirName = "no such commend";
                break;
        }
        return dirName;
    }

    private static String toImage(String fileName) throws IOException {
        PDDocument document = PDDocument.load(new File(fileName));
        PDFRenderer pdfRenderer = new PDFRenderer(document);

        for (int page = 0; page < document.getNumberOfPages() && page < 1; ++page) {
            BufferedImage bim = pdfRenderer.renderImageWithDPI(
                    page, 300, ImageType.RGB);
            ImageIOUtil.writeImage(
                    bim, String.format("src/output/pdf-%d.%s", page + 1, "png"), 300);
        }
        document.close();
        return "src/output/pdf-%d.%s";
    }

    private static String toHTML(String fileName) throws IOException, ParserConfigurationException {
        PDDocument pdf = PDDocument.load(new File(fileName));
        PDPage page = pdf.getPage(0);
        PDDocument firstPage = new PDDocument();
        firstPage.addPage(page);
        Writer output = new PrintWriter("src/output/pdf.html", "utf-8");
        new PDFDomTree().writeText(firstPage, output);
        output.close();
        return "src/output/pdf.html";
    }

    private static String toText(String fileName) throws IOException {
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
        return "src/output/pdf.txt";
    }
}