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


public class WorkerMain {

    public static final int MALFORMED_URL_EXCEPTION = 1;
    public static final int IO_Exception = 2;
    public static final int SUCCESS_TO_DOWONLOAD = 3;
    public static final String currDir = System.getProperty("user.dir");

    // todo - there should be 2 sqs queues : one for sending and one for receiving- the one for receiving is at line 34 and 56 all the rest is the sending sqs-queue;
    // todo - If a worker stops working unexpectedly before finishing its work on a message, then some other worker should be able to handle that message.
    public static void main(String[] args) throws IOException, ParserConfigurationException {
        String inputQueueUrl = args[1];
        String outputQueueUrl = args[2];
        String bucketName = args[3];

        S3Adapter s3Adapter = new S3Adapter();
        SQSAdapter sqsAdapter = new SQSAdapter();

        List<Message> messages = sqsAdapter.retrieveOneMessage(inputQueueUrl); //todo is that the queueurl?
        String fileDir = null;
        for (Message m : messages) {
            String[] data = m.body().split(",");
            try {
                fileDir = handleMessage(data[0], data[1], sqsAdapter, outputQueueUrl);
                if (fileDir == "no such commend") {
                    sqsAdapter.sendMessage(outputQueueUrl, data[1] + "," + data[0] + ",do not support " + data[1]); //todo is that the queue-url?
                } else if (fileDir == "could not download the file") {
                    continue;
                } else {
                    String key = fileDir;
                    s3Adapter.putFileInBucketFromPath(bucketName, key, fileDir);
                    String fileUrl = String.format(Main.S3filePathFormat, bucketName, key);
                    sqsAdapter.sendMessage(outputQueueUrl, fileUrl); //todo - chancge the s3url and queuee-url
                    File f = new File(fileDir + "\\downloadPDFFIle.pdf");//todo - delete the file
                    f.delete();
                }

            } catch (Exception e) {
                sqsAdapter.sendMessage(outputQueueUrl, data[1] + "," + data[0] + ",has failed duo to ParserConfigurationException or IOException"); //todo is that the queue-url?
            }
        }
        sqsAdapter.deleteMessage(messages, inputQueueUrl); // todo i think that shuold be anther sqs
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

    private static String handleMessage(String url, String action, SQSAdapter sqsAdapter, String outputQueueUrl) throws IOException, ParserConfigurationException {
        String dirName = "";
        if (saveFileFromUrlWithJavaIO(currDir + "\\downloadPDFFIle.pdf", url) == MALFORMED_URL_EXCEPTION) {
            sqsAdapter.sendMessage(outputQueueUrl, action + "," + url + ":the url is malformed"); //todo is that the queue-url?
            return "could not download the file";

        } else if (saveFileFromUrlWithJavaIO(currDir + "\\downloadPDFFIle.pdf", url) == IO_Exception) {
            sqsAdapter.sendMessage(outputQueueUrl, action + "," + url + ":IOException has happened"); //todo is that the queue-url?
            return "could not download the file";
        }
        switch (action) {
            case "ToImage":
                dirName = toImage(currDir + "\\downloadPDFFIle.pdf");
                break;
            case "ToHTML":
                dirName = toHTML(currDir + "\\downloadPDFFIle.pdf");
                break;
            case "ToText":
                dirName = toText(currDir + "\\downloadPDFFIle.pdf");
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
        PDFParser parser = new PDFParser((RandomAccessRead) new RandomAccessFile(f, "r"));
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