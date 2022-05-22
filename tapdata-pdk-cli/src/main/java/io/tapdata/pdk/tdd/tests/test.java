package io.tapdata.pdk.tdd.tests;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeUtils;
import io.tapdata.pdk.core.tapnode.TapNodeContainer;
import org.apache.commons.io.FileUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFCreationHelper;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileOutputStream;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class test {
    private static XSSFWorkbook workbook;
	public static void main(String... args) throws Throwable {
        if(workbook == null)
            workbook = new XSSFWorkbook();
        CreationHelper creationHelper = (XSSFCreationHelper) workbook.getCreationHelper();
        //Create a blank sheet
        XSSFSheet sheet = workbook.createSheet("test");
//        sheet.setDefaultColumnWidth(3000);
//        sheet.setDefaultRowHeight((short) 300);
//        sheet.setAutobreaks(true);
        //This data needs to be written (Object[])
        Map<String, Object[]> data = new LinkedHashMap<>();
        data.put("1", new Object[] {"No.", "Source type", "Source model", "Target type", "Target model"});
        data.put("2", new Object[] {"No.1", "Source type1", "Source model1", "Target type1", "Target model1"});

        //https://www.baeldung.com/apache-poi-change-cell-font
        //Iterate over data and write to sheet
        Set<String> keyset = data.keySet();
        int rownum = 0;
        for (String key : keyset)
        {
            Row row = sheet.createRow(rownum++);
            Object [] objArr = data.get(key);
            int cellnum = 0;
            for (Object obj : objArr)
            {
                Cell cell = row.createCell(cellnum++);
                sheet.autoSizeColumn(cellnum - 1);
//                Drawing drawing = sheet.createDrawingPatriarch();
//                CreationHelper factory = workbook.getCreationHelper();
//                ClientAnchor anchor = factory.createClientAnchor();
//                anchor.setCol1(rownum);
//                anchor.setCol2(rownum + 3);
//                anchor.setRow1(row.getRowNum());
//                anchor.setRow2(row.getRowNum() + 4);
//                Comment comment = drawing.createCellComment(anchor);
                //set the comment text and author
//                comment.setString(factory.createRichTextString("aaaaaaaaaaaakldjflkajfkldsfjalkf"));
//                comment.setAuthor("Aplomb");
//                cell.setCellComment(comment);

                Drawing drawing = sheet.createDrawingPatriarch();
                ClientAnchor clientAnchor = drawing.createAnchor(0, 0, 0, 0, 0, 2, 7, 12);

                Comment comment = (Comment) drawing.createCellComment(clientAnchor);

                RichTextString richTextString = creationHelper.createRichTextString(
                        "We can put a long comment here with \n a new line text followed by another \n new line text");

                comment.setString(richTextString);
                comment.setAuthor("Aplomb");

                cell.setCellComment(comment);


                CellStyle cellStyle = workbook.createCellStyle();
                cellStyle.setFillForegroundColor(IndexedColors.LIGHT_BLUE.getIndex());
                cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
                cell.setCellStyle(cellStyle);
                if(obj instanceof String)
                    cell.setCellValue((String)obj);
                else if(obj instanceof Integer)
                    cell.setCellValue((Integer)obj);
            }
        }

        writeWorkbook();
    }
    public static void autoSizeColumns(Workbook workbook) {
        int numberOfSheets = workbook.getNumberOfSheets();
        for (int i = 0; i < numberOfSheets; i++) {
            Sheet sheet = workbook.getSheetAt(i);
            if (sheet.getPhysicalNumberOfRows() > 0) {
                Row row = sheet.getRow(sheet.getFirstRowNum());
                Iterator<Cell> cellIterator = row.cellIterator();
                while (cellIterator.hasNext()) {
                    Cell cell = cellIterator.next();
                    int columnIndex = cell.getColumnIndex();
                    sheet.autoSizeColumn(columnIndex);
                }
            }
        }
    }
    private static void writeWorkbook() {
        if(workbook == null)
            return;
        try
        {
            //Write the workbook in file system
            FileOutputStream out = FileUtils.openOutputStream(new File("./output/" + "report_" + dateTime() + ".xlsx"));
            workbook.write(out);
            out.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private static String dateTime() {
        return DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss_SSS").withZone(ZoneId.systemDefault()).format(new Date().toInstant());
    }
	public static BigDecimal maxValueForPrecision(int maxPrecision) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < maxPrecision; i++) {
			builder.append("9");
		}
		return new BigDecimal(builder.toString());
	}

	public static BigDecimal minValueForPrecision(int maxPrecision) {
		StringBuilder builder = new StringBuilder("-");
		for (int i = 0; i < maxPrecision; i++) {
			builder.append("9");
		}
		return new BigDecimal(builder.toString());
	}
}
