package mains;

import adapters.S3Adapter;
import adapters.SQSAdapter;
import org.apache.pdfbox.cos.COSDocument;
import org.apache.pdfbox.io.RandomAccessFile;
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
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;


public class WorkerMain {

    public static final int MALFORMED_URL_EXCEPTION = 1;
    public static final int IO_Exception = 2;
    public static final int SUCCESS_TO_DOWONLOAD = 3;
    public static final String currDir = System.getProperty("user.dir");
    public static String localAppSqs;

    // todo - If a worker stops working unexpectedly before finishing its work on a message, then some other worker should be able to handle that message.
    public static void main(String[] args) throws IOException, ParserConfigurationException {
        String inputQueueUrl = args[0];
        String outputQueueUrl = args[1];
        String bucketName = args[2];

        S3Adapter s3Adapter = new S3Adapter();
        SQSAdapter sqsAdapter = new SQSAdapter();

        List<Message> messages = new ArrayList<>();
        while (true) {
            do {
                messages = sqsAdapter.retrieveMessage(inputQueueUrl, 1, 20);
            } while (messages.isEmpty());
            String fileDir = null;

            Message m = messages.get(0);
            String[] data = m.body().split(",");
            String url = data[0];
            String action = data[1];
            localAppSqs = data[2];

            try {
                fileDir = handleMessage(url, action, sqsAdapter, outputQueueUrl);
                if (fileDir == "no such command") {
                    sqsAdapter.sendMessage(outputQueueUrl, action + "," + url + ",do not support " + action + "," + localAppSqs + ",fail");
                } else if (fileDir == "could not download the file") {
                    sqsAdapter.deleteMessage(messages, inputQueueUrl);
                } else {
                    String key = "key" + fileDir;
                    s3Adapter.putFileInBucketFromPath(bucketName, key, fileDir);
                    sqsAdapter.sendMessage(outputQueueUrl, String.format("%s,%s,%s,%s,%s", url, key, localAppSqs, action, "success"));
                    File f = new File(fileDir);
                    f.delete();
                }

            } catch (Exception e) {
                e.printStackTrace();
                sqsAdapter.sendMessage(outputQueueUrl, action + "," + url + ",has failed duo to ParserConfigurationException or IOException," + localAppSqs + ",fail");
            }
            sqsAdapter.deleteMessage(messages, inputQueueUrl);
        }
    }

    private static int saveFileFromUrlWithJavaIO(String fileName, String fileUrl) {
        try {
            ReadableByteChannel readChannel = Channels.newChannel(new URL(fileUrl).openStream());
            FileOutputStream fileOS = new FileOutputStream(fileName);
            FileChannel writeChannel = fileOS.getChannel();
            writeChannel
                    .transferFrom(readChannel, 0, Long.MAX_VALUE);

        } catch (MalformedURLException e) {
            e.printStackTrace();
            return MALFORMED_URL_EXCEPTION;

        } catch (IOException e) {
            e.printStackTrace();
            return IO_Exception;

        }
        return SUCCESS_TO_DOWONLOAD;
    }

    private static String handleMessage(String url, String action, SQSAdapter sqsAdapter, String outputQueueUrl) throws
            IOException, ParserConfigurationException {
        String dirName = "";
        int ack = saveFileFromUrlWithJavaIO(currDir + "/downloadPDFFIle.pdf", url);
        if (ack == MALFORMED_URL_EXCEPTION) {
            sqsAdapter.sendMessage(outputQueueUrl, action + "," + url + ",the url is malformed," + localAppSqs + ",fail"); //todo is that the queue-url?
            return "could not download the file";

        } else if (ack == IO_Exception) {
            sqsAdapter.sendMessage(outputQueueUrl, action + "," + url + ",IOException has happened," + localAppSqs + ",fail"); //todo is that the queue-url?
            return "could not download the file";
        }
        switch (action) {
            case "ToImage":
                dirName = toImage(currDir + "/downloadPDFFIle.pdf");
                break;
            case "ToHTML":
                dirName = toHTML(currDir + "/downloadPDFFIle.pdf");
                break;
            case "ToText":
                dirName = toText(currDir + "/downloadPDFFIle.pdf");
                break;
            default:
                dirName = "no such command";
                break;
        }
        File f = new File(currDir + "/downloadPDFFIle.pdf");
        f.delete();
        return dirName;
    }

    private static String toImage(String fileName) throws IOException {
        PDDocument document = PDDocument.load(new File(fileName));
        PDFRenderer pdfRenderer = new PDFRenderer(document);

        String outPath = currDir + "/" + System.currentTimeMillis() + " pdf.png";


        BufferedImage bim = pdfRenderer.renderImageWithDPI(
                0, 300, ImageType.RGB);
        ImageIOUtil.writeImage(
                bim, outPath, 300);
        document.close();
        return outPath;
    }

    private static String toHTML(String fileName) throws IOException, ParserConfigurationException {
        PDDocument pdf = PDDocument.load(new File(fileName));
        PDPage page = pdf.getPage(0);
        PDDocument firstPage = new PDDocument();
        firstPage.addPage(page);
        String outPath = currDir + "/" + System.currentTimeMillis() + "pdf.html";
        Writer output = new PrintWriter(outPath, "utf-8");
        new PDFDomTree().writeText(firstPage, output);
        output.close();
        return outPath;
    }

    private static String toText(String fileName) throws IOException {
        File f = new File(fileName);
        String parsedText;
        PDFParser parser = new PDFParser(new RandomAccessFile(f, "r"));
        parser.parse();
        COSDocument cosDoc = parser.getDocument();
        PDFTextStripper pdfStripper = new PDFTextStripper();
        PDDocument pdDoc = new PDDocument(cosDoc);
        PDPage page = pdDoc.getPage(0);
        PDDocument firstPage = new PDDocument();
        firstPage.addPage(page);
        parsedText = pdfStripper.getText(firstPage);
        String outPath = currDir + "/" + System.currentTimeMillis() + "pdf.txt";
        PrintWriter pw = new PrintWriter(outPath);
        pw.print(parsedText);
        pw.close();
        return outPath;
    }
}