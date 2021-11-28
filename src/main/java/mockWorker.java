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

import javax.xml.parsers.ParserConfigurationException;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;


public class mockWorker {

    public static void main(String[] args) throws IOException, ParserConfigurationException {
        handleMessage("http://www.pdf995.com/samples/pdf.pdf", "ToImage");
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


    /* private static void saveFileFromUrlWithJavaIO(String fileName, String fileUrl) {
         try {
             //String arg = args[0];
             URL url = new URL(fileUrl);
             //jpg
             InputStream in = new BufferedInputStream(url.openStream());
             ByteArrayOutputStream out = new ByteArrayOutputStream();
             byte[] buf = new byte[131072];
             int n = 0;
             while (-1 != (n = in.read(buf))) {
                 out.write(buf, 0, n);
             }
             out.close();
             in.close();
             byte[] response = out.toByteArray();
             FileOutputStream fos = new FileOutputStream(fileName);
             fos.write(response);
             fos.close();
         } catch (Exception e) {
             e.printStackTrace();
         }
     }*/
    private static void handleMessage(String url, String action) throws IOException, ParserConfigurationException {
        String dirName = "/Users/eladweinfeld/Desktop/testaws/pdfTOTest.pdf";
        saveFileFromUrlWithJavaIO(dirName, url);
        switch (action) {
            case "ToImage":
                toImage(dirName);
                break;
            case "ToHTML":
                toHTML(dirName);
                break;
            case "ToText":
                toText(dirName);
                break;
            default:
                //toDo - what to do if no good action - need to If an exception occurs while performing an operation on a PDF file, or the PDF file is not available, then output line for this file will be: <operation>: input file <a short description of the exception>.
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
                    bim, String.format("src/main/java/pdf-%d.%s", page + 1, "png"), 300);
        }
        document.close();
    }

    private static void toHTML(String fileName) throws IOException, ParserConfigurationException {
        PDDocument pdf = PDDocument.load(new File(fileName));
        PDPage page = pdf.getPage(0);
        PDDocument firstPage = new PDDocument();
        firstPage.addPage(page);
        Writer output = new PrintWriter("/Users/eladweinfeld/Desktop/testaws/dsfsdf/pdf.html", "utf-8");
        new PDFDomTree().writeText(firstPage, output);
        output.close();
    }

    private static void toText(String fileName) throws IOException, ParserConfigurationException {
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
        PrintWriter pw = new PrintWriter("src/main/java/pdf.txt");
        pw.print(parsedText);
        pw.close();
    }


}
