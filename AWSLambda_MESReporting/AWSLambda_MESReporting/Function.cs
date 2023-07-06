using Amazon.Lambda.Core;
using Amazon.S3.Model;
using Amazon.S3; 

using System.Data.SqlClient;
using System.Data;
using System.Diagnostics;
using System.IO;

using CsvHelper;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using System.Runtime.CompilerServices;
using Amazon.S3.Model.Internal.MarshallTransformations;
using System.Data.Common;


using MimeKit;
using System.Net;
using System.Net.Mail;
using System.Net.Mime;
using DocumentFormat.OpenXml.Drawing;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AWSLambda_MESReporting;

public class Function
{
    string currentdatetime_tmp = DateTime.Now.ToString("yyyyMMddHHmmss");

    /// <summary>
    /// A simple function that takes a string and does a ToUpper
    /// </summary>
    /// <param name="input"></param>
    /// <param name="context"></param>
    /// <returns></returns> 
    public class FunctionInput
    {
        public string query { get; set; }
        /*
        {
            "password": "RPT-j3ms^cust"
        }
        */
    }

    private DataSet GetConnection(string ConnectionString, string Command)
    {
        using (SqlConnection cn = new SqlConnection(ConnectionString))
        {
            cn.Open();
            SqlDataAdapter adapter = new SqlDataAdapter(Command, ConnectionString);
            DataSet df = new DataSet();
            adapter.Fill(df);
            Console.Write(df);

            return df;
        }

    }

    private string ConvertDatasetToCsv(DataSet dataset)
    {
        string fileName = currentdatetime_tmp + "ExportedData.csv";
        string filePath = @"/tmp/"+fileName;
            //@"C:/Users/3642091/Documents/Data Conversion .NET Core/AWSLambda_excel/AWSLambda_excel/ExcelFile/" + currentdatetime_tmp + "ExportedData.csv";

        if ((dataset == null) || (dataset.Tables.Count == 0))
        {
            Console.WriteLine("Dataset is null or empty.");
        }

        DataTable table = dataset.Tables[0];

        if (table.Rows.Count == 0)
        {
            Console.WriteLine("DataTable is empty.");
        }

        using (StreamWriter writer = new StreamWriter(filePath))
        using (CsvWriter csvwriter = new CsvWriter(writer, System.Globalization.CultureInfo.InvariantCulture))
        {
            //csvwriter.WriteRecords(table.CreateDataReader());
            //Write column names 
            for (int i = 0; i < table.Columns.Count; i++)
            {
                writer.Write(table.Columns[i].ColumnName);
                if (i < table.Columns.Count - 1)
                {
                    writer.Write(",");
                }
            }
            writer.WriteLine();

            //Write data rows
            foreach (DataRow row in table.Rows)
            {
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    writer.Write(row[i].ToString());
                    if (i < table.Columns.Count - 1)
                    {
                        writer.Write(",");
                    }
                }
                writer.WriteLine();
            }

        }

        Console.WriteLine($"CSV file created successfully at: {filePath}");
        return fileName;
    }
    private string CovertDatasetToExcel(DataSet dataset)
    {
        string fileName = currentdatetime_tmp + "ExportedData.xlsx";
        string filePath = $@"/tmp/" + fileName;
        //"C:/Users/3642091/Documents/Data Conversion .NET Core/AWSLambda_excel/AWSLambda_excel/ExcelFile/" + currentdatetime_tmp + "ExportedData.xlsx";

        using (SpreadsheetDocument spreadsheet = SpreadsheetDocument.Create(filePath, SpreadsheetDocumentType.Workbook))
        {
            WorkbookPart workbookPart = spreadsheet.AddWorkbookPart();
            workbookPart.Workbook = new Workbook();

            WorksheetPart worksheetPart = workbookPart.AddNewPart<WorksheetPart>();
            worksheetPart.Worksheet = new Worksheet(new SheetData());

            Sheets sheets = spreadsheet.WorkbookPart.Workbook.AppendChild<Sheets>(new Sheets());
            Sheet sheet = new Sheet()
            {
                Id = spreadsheet.WorkbookPart.GetIdOfPart(worksheetPart),
                SheetId = 1,
                Name = "MES Reporting"
            };

            sheets.Append(sheet);
            Worksheet worksheet = worksheetPart.Worksheet;
            SheetData sheetData = worksheet.GetFirstChild<SheetData>();

            DataTable table = dataset.Tables[0];

            //Write columns name
            Row headerRow = new Row();

            foreach (DataColumn column in table.Columns)
            {
                //Cell cell = new Cell(new InlineString(new Text(column.ColumnName))) { DataType = CellValues.String };
                Cell cell = new Cell()
                {
                    CellValue = new CellValue(column.ColumnName),
                    DataType = CellValues.String
                };
                //Console.WriteLine($"{column.ColumnName}");
                headerRow.AppendChild(cell);
            }
            sheetData.AppendChild(headerRow);

            //Write data rows
            foreach (DataRow row in table.Rows)
            {
                Row r = new Row();

                for (int i = 0; i < row.ItemArray.Length; i++)
                {
                    Cell c = new Cell()
                    {
                        CellValue = new CellValue(row[i].ToString()),
                        DataType = CellValues.String
                    };
                    r.Append(c);
                }
                sheetData.Append(r);
            }

            worksheetPart.Worksheet.Save();

            spreadsheet.Close();

        }

        Console.WriteLine($"Excel file created successfully at: {filePath}");
        return fileName;
    }

    public static async Task<bool> StoreOnS3Async(string fileName)
    {
        var s3Client = new AmazonS3Client();
        IAmazonS3 client = s3Client;
        string bucketName = "jbl-ems-southasia-python-poc";
        string objectName = fileName;
        string filePath = $@"/tmp/" + fileName;

        var request = new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectName,
            FilePath = filePath,
        };

        var response = await client.PutObjectAsync(request);
        if (response.HttpStatusCode == System.Net.HttpStatusCode.OK)
        {
            Console.WriteLine($"Successfully uploaded {objectName} to {bucketName}.");
            return true;
        }
        else
        {
            Console.WriteLine($"Could not upload {objectName} to {bucketName}.");
            return false;
        }
    }

    private static bool ValidateCertificate(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
    {
        // Add custom certificate validation logic here
        return true; // Return true to bypass certificate validation
    }

    private async Task<bool> FileEmailAttachment(string csv, string excel)
    {
        Console.WriteLine("Sending Email....");

        string API_KEY = Environment.GetEnvironmentVariable("API_KEY");
        string EMAIL_FROM = Environment.GetEnvironmentVariable("EMAIL_FROM");
        string EMAIL_TO = Environment.GetEnvironmentVariable("EMAIL_TO");

        string SMTP_SERVER = Environment.GetEnvironmentVariable("SMTP_SERVER");
        int SMTP_PORT = Int32.Parse(Environment.GetEnvironmentVariable("SMTP_PORT"));
        string SMTP_PWD = Environment.GetEnvironmentVariable("SMTP_PWD");
        string subject = "MES Reporting Test Email";
        string bodyHtml = "This is an email with .csv and .xlsx attachment from MES Reporting.";
        string csvPath = $@"/tmp/{csv}";
        string excelPath = $@"/tmp/{excel}";

        using var message = new MailMessage();
        message.From = new MailAddress(
            EMAIL_FROM,
            "Yi ying"
        );
        message.To.Add(new MailAddress(
            EMAIL_TO,
            "Anonymous"));

        message.Subject = "MES Reporting POC (.NET)";
        var textBody = "This is a file with attachment of csv and xlsx file from JEMS database.";
        message.AlternateViews.Add(AlternateView.CreateAlternateViewFromString(
            textBody, null, MediaTypeNames.Text.Html));

        //Attach csv and xlsx file
        var csvAttachment = new System.Net.Mail.Attachment(csvPath);
        message.Attachments.Add(csvAttachment);

        var excelAttachment = new System.Net.Mail.Attachment(excelPath);
        message.Attachments.Add(excelAttachment);

        //Configure the SMTP client
        using var client = new SmtpClient(host: SMTP_SERVER,port: SMTP_PORT);
        //Authentication testing
        ServicePointManager.ServerCertificateValidationCallback = ValidateCertificate;
        client.UseDefaultCredentials = false;
        client.Credentials = new NetworkCredential(
            userName: EMAIL_FROM,
            password: SMTP_PWD);

        try
        {
            await client.SendMailAsync(message);
            Console.WriteLine("Email sent successfully.");
            return true;
        }
        catch(Exception ex)
        {
            Console.WriteLine($"Certificate validation error: {ex}");
            throw;
        }
        
    }

    public async Task<string> FunctionHandler(FunctionInput input, ILambdaContext context)
    {
        //database key variables
        //string server = "AZAPSEPENSQL81";
        //string database = "JEMS";
        //string username = "MES_CustReports";
        //string password = "RPT-j3ms^cust";
        //string Query = "exec up_RE_Rpt_Assemblies 9, 28884, 0; "; //"select * from RE_ReportServer;\";
        //dbConnection = @"Server=AZAPSEPENSQL81;Database=JEMS;Uid=MES_CustReports;Password=RPT-j3ms^cust";

        string Query = input.query;
        Console.Write(Query);

        if (Query != null)
        {
            string CONN_KEY = Environment.GetEnvironmentVariable("CONN_KEY");
            

            DataSet dataset = GetConnection(CONN_KEY, Query);
            
            //check dataset
            foreach (DataTable tb in dataset.Tables)
            {
                Console.WriteLine($"Table: {tb.TableName}");

                foreach (DataRow row in tb.Rows)
                {
                    foreach (DataColumn column in tb.Columns)
                    {
                        Console.WriteLine($"{column.ColumnName}: {row[column]}");
                    }
                    Console.WriteLine();
                }
            }

            //Data Conversion
            string csvFile = ConvertDatasetToCsv(dataset);
            string excelFile = CovertDatasetToExcel(dataset);

            //Store file to S3 Bucket
            StoreOnS3Async(csvFile);
            StoreOnS3Async(excelFile);

            //Email Attachment
            bool send_status = false;
            if ((csvFile != null) && (excelFile != null))
            {
                //Console.WriteLine("Call Email Attachment Function on main....");
                send_status = await FileEmailAttachment(csvFile, excelFile);
            }     

            if (send_status)
            {
                return $"COMPLETE!";

            }
            else
            {
                return $"An Error Occured!";
            }
            
        }

        else
        {
            return "Query is Empty!";
        }
        
    }
}
