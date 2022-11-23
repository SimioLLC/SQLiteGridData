using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Data.Common;
using System.Globalization;
using SimioAPI.Extensions;
using System.Text.RegularExpressions;
using System.Data.SQLite;

namespace SQLiteGridData
{
    public class ExporterDefinition : IGridDataExporterDefinition
    {
        public string Name => "SQLite Data Exporter";
        public string Description => "An exporter to SQLite";
        public Image Icon => null;

        static readonly Guid MY_ID = new Guid("b6966fed-6147-4bae-8ebf-352d00b57bfd");
        public Guid UniqueID => MY_ID;

        public IGridDataExporter CreateInstance(IGridDataExporterContext context)
        {
            return new Exporter(context);
        }

        public void DefineSchema(IGridDataSchema schema)
        {
            var connectionStringProp = schema.OverallProperties.AddStringProperty("ConnectionString");
            connectionStringProp.DisplayName = "Connection String";
            connectionStringProp.Description = "The server Connection String.";
            connectionStringProp.DefaultValue = "URI=file:C:\\temp\\test.db";

            var connectionTimeOutProp = schema.OverallProperties.AddStringProperty("ConnectionTimeOut");
            connectionTimeOutProp.DisplayName = "Connection TimeOut (seconds)";
            connectionTimeOutProp.Description = "Connection TimeOut in Seconds.";
            connectionTimeOutProp.DefaultValue = "30";

            var dateTimeFormatProp = schema.OverallProperties.AddStringProperty("DateTimeFormat");
            dateTimeFormatProp.DisplayName = "DateTime Format";
            dateTimeFormatProp.Description = "DateTime Format Used To Save To Database (e.g. yyyy-MM-dd HH:mm:ss).  String value need to be defined.";
            dateTimeFormatProp.DefaultValue = "yyyy-MM-dd HH:mm:ss";

            var dataBaseTableNameProp = schema.PerTableProperties.AddStringProperty("DatabaseTableName");
            dataBaseTableNameProp.DisplayName = "Database Table Name";
            dataBaseTableNameProp.Description = "The Database table to write to";
            dataBaseTableNameProp.DefaultValue = String.Empty;
        }
    }

    class Exporter : IGridDataExporter
    {
        public Exporter(IGridDataExporterContext context)
        {
        }

        private object _sync = new object();

        public OpenExportDataResult OpenData(IGridDataOpenExportDataContext openContext)
        {
            GetValues(openContext.GridDataName, openContext.Settings, out var connectionString, out var connectionTimeOut, out var dateTimeFormat, out var databaseTableName);

            if (String.IsNullOrWhiteSpace(connectionString))
                return OpenExportDataResult.Failed("The Connection String parameter is not specified");

            if (connectionTimeOut <= 0)
                return OpenExportDataResult.Failed("The Connection TimeOut parameter needs to be greater than zero");

            if (String.IsNullOrWhiteSpace(dateTimeFormat))
                return OpenExportDataResult.Failed("The DateTime Format parameter is not specified");

            if (String.IsNullOrWhiteSpace(databaseTableName))
                return OpenExportDataResult.Failed("The Database Table Name parameter is not specified");            

            // Lock around this to allow only one thread to export using the exporter at a time, in cases like creating a table if it doesn't exist or whatnot.
            //  This supposes most use cases will have a single connection string per exporter. If exporters share a connection string, this might not entirely 
            //  work, and we might need to sync a different way.
            lock (_sync)
            {
                using (var ExporterConnection = new ExporterConnection(connectionString, connectionTimeOut, dateTimeFormat))
                {
                    SaveExportContextToDatabaseTable(ExporterConnection, ExporterConnection.DateTimeFormat, openContext, databaseTableName);
                }
            }

            return OpenExportDataResult.Succeeded();
        }

        public string GetDataSummary(IGridDataSummaryContext context)
        {
            if (context == null)
                return null;

            GetValues(context.GridDataName, context.Settings, out var connectionString, out _, out _, out var databaseTableName);

            if (databaseTableName == null)
                return null;

            return String.Format("Exporting to {0} : {1} table", connectionString, databaseTableName);
        }

        private static void GetValues(string tableName, IGridDataOverallSettings settings, out string connectionString, out int connectionTimeOut, out string dateTimeFormat, out string databaseTableName)
        {
            connectionString = (string)settings.Properties["ConnectionString"].Value;
            connectionTimeOut = Convert.ToInt32(settings.Properties["ConnectionTimeOut"].Value);
            dateTimeFormat = (string)settings.Properties["DateTimeFormat"]?.Value;
            databaseTableName = (string)settings.GridDataSettings[tableName]?.Properties["DatabaseTableName"]?.Value;
        }

        public void Dispose()
        {
        }

        public static string BuildSqlCreateCommandFromExportContext(ExporterConnection ExporterConnection, IGridDataOpenExportDataContext exportContext, string databaseTableName)
        {
            try
            {
                bool firstColumn = true;
                string sqlCreate = $"CREATE TABLE {databaseTableName} (";
                // Add Property Columns
                foreach (var col in exportContext.Records.Columns)
                {
                    if (firstColumn == false)
                    {
                        sqlCreate += ", ";
                    }
                    else
                    {
                        firstColumn = false;
                    }

                    if (col.IsKey)
                    {
                        sqlCreate += $"[{col.Name}] {GetExportDataColumnType(ExporterConnection, col)} not null primary key";
                    }
                    else
                    {
                        if (col.DefaultValue != null && col.DefaultString.Length > 0)
                        {
                            DbColumnInfo dbColumnInfo = new DbColumnInfo(col.Name, col.Type, true);
                            string formattedValue = GetFormattedStringValue(ExporterConnection, col.DefaultString, null, col);

                            sqlCreate += $"[{col.Name}] {GetExportDataColumnType(ExporterConnection, col)} null Default '{formattedValue}'";                                
                        }
                        else
                        {
                            sqlCreate += $"[{col.Name}] {GetExportDataColumnType(ExporterConnection, col)} null";
                        }
                    }
 
                }

                if (firstColumn == true)
                    throw new Exception("No Columns Available To Create Table");

                sqlCreate += ")";

                return sqlCreate;
            }
            catch (Exception ex)
            {
                throw new ApplicationException($"Cannot build SQL CREATE command. Err={ex}");
            }
        }

        public static void SaveExportContextToDatabaseTable(ExporterConnection ExporterConnection, String dateTimeFormat, IGridDataOpenExportDataContext exportContext, string databaseTableName)
        {
            try
            {
                using (var cmd = ExporterConnection.Connection.CreateCommand())
                {
                    string sqlDrop = $"DROP TABLE IF EXISTS {databaseTableName}";
                    cmd.CommandText = sqlDrop;
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandTimeout = ExporterConnection.ConnectionTimeOut;
                    cmd.ExecuteNonQuery();
                }

                using (var cmd = ExporterConnection.Connection.CreateCommand())
                {
                    string sqlCreateCommand = BuildSqlCreateCommandFromExportContext(ExporterConnection, exportContext, databaseTableName);
                    cmd.CommandText = sqlCreateCommand;
                    cmd.CommandTimeout = ExporterConnection.ConnectionTimeOut;
                    cmd.ExecuteNonQuery();
                }

                // get db column names...These are retrieved from database in the GetColumnInfoForTable method
                List<DbColumnInfo> sqlColumnInfoList = new List<DbColumnInfo>(SQLiteGridDataUtils.GetColumnInfoForTable(ExporterConnection.Connection,
                    databaseTableName, ExporterConnection.ConnectionTimeOut));

                // get table
                var dt = ConvertExportContextToDataTable(ExporterConnection, exportContext, sqlColumnInfoList);

                // write datatable to database using inserts
                using (var transaction = ExporterConnection.Connection.BeginTransaction())
                {
                    foreach (DataRow dataRow in dt.Rows)
                    {
                        string rowValues = String.Empty;
                        foreach (DataColumn dataColumn in dt.Columns)
                        {
                            if (rowValues.Length > 0)
                                rowValues += ", ";

                            var dataValue = dataRow[dataColumn];
                            if ((dataValue == null || Convert.ToString(dataValue).Length == 0) && dataColumn.AllowDBNull)
                            {
                                rowValues += "NULL";
                            }
                            else
                            {
                                if (dataColumn.DataType.IsPrimitive) // int, bool, double, float, etc...
                                {
                                    object typedValue = null;
                                    if (dataValue == null)
                                    {
                                        // No data value given, however we failed the AllowDBNull check above, so 
                                        //  no null values allowed, just give them the default vaue for the type
                                        typedValue = Activator.CreateInstance(dataColumn.DataType);
                                    }
                                    else
                                    {
                                        typedValue = Convert.ChangeType(dataValue, dataColumn.DataType, CultureInfo.InvariantCulture);
                                    }

                                    // Using InvariantCulture because decimal separator must be ".", something like "," would be invalid SQL
                                    var typedStringValue = String.Format(CultureInfo.InvariantCulture, "{0}", typedValue);
                                    if (typedStringValue == "Infinity")
                                        typedStringValue = "'+Infinity'";
                                    if (typedStringValue == "-Infinity") 
                                        typedStringValue = "'-Infinity'";
                                    rowValues += typedStringValue;

                                }
                                else
                                {
                                    string colValue = Convert.ToString(dataValue ?? String.Empty);
                                    if (colValue.Length > 0)
                                        rowValues += $"'{colValue}'";
                                    else
                                        rowValues += "''";
                                }
                            }
                        }

                        using (var cmd = ExporterConnection.Connection.CreateCommand())
                        {
                            var sql = $"INSERT INTO {databaseTableName} VALUES ( {rowValues} )";
                            cmd.CommandTimeout = ExporterConnection.ConnectionTimeOut;
                            cmd.CommandText = sql;
                            cmd.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                }
            }
            catch (Exception ex)
            {
                throw new ApplicationException($"There was a problem exporting. Table={databaseTableName} Err={ex}");
            }               
        }

        internal static DataTable ConvertExportContextToDataTable(ExporterConnection ExporterConnection, IGridDataOpenExportDataContext exportContext, List<DbColumnInfo> dbColumnInfoList)
        {
            // New table
            var dataTable = new DataTable();
            dataTable.TableName = exportContext.GridDataName;
            dataTable.Locale = CultureInfo.InvariantCulture;

            List<int> colExportRecordIndices = new List<int>();
            List<IGridDataExportColumnInfo> colExportColumnInfo = new List<IGridDataExportColumnInfo>();
            
            // For each column in the actual DB table...
            foreach (var dbColumnInfo in dbColumnInfoList)
            {
                // Find the corresponding column in the export records...
                int exportRecordIndex = 0;
                foreach (var col in exportContext.Records.Columns)
                {
                    if (dbColumnInfo.Name.ToLowerInvariant() == col.Name.ToLowerInvariant())
                    {
                        // When (and if) we find the column, then record the index into 
                        //  the export records we found the record column for this given 
                        //  DB column
                        colExportRecordIndices.Add(exportRecordIndex);
                        colExportColumnInfo.Add(col);
                        break;
                    }
                    exportRecordIndex++;
                }

                // add columns
                var dtCol = dataTable.Columns.Add(dbColumnInfo.Name, dbColumnInfo.Type);                
            }

            // Add Rows to data table
            foreach (var record in exportContext.Records)
            {
                object[] thisRow = new object[dataTable.Columns.Count];

                // For each defined DB column...
                int dbColIndex = 0;
                int dataTableColumnIndex = 0;
                foreach (var dbColumnInfo in dbColumnInfoList)
                {
                    // Get the index of the corresponding column in the export records
                    int exportRecordColIdx = colExportRecordIndices[dbColIndex];
                    IGridDataExportColumnInfo exportRecordColtype = colExportColumnInfo[dbColIndex]; ;

                    if (exportRecordColIdx >= 0)
                    {
                        // There was a corresponding export records column, and it was enabled, so export its value
                        var valueStr = record.GetString(exportRecordColIdx);
                        var valueObj = record.GetNativeObject(exportRecordColIdx);                                               
                        string formattedValue = GetFormattedStringValue(ExporterConnection, record.GetString(exportRecordColIdx), record.GetNativeObject(exportRecordColIdx), exportRecordColtype);
                        thisRow[dataTableColumnIndex] = formattedValue;
                        dataTableColumnIndex++;
                    }
                    dbColIndex++;
                }

                dataTable.Rows.Add(thisRow);
            }

            return dataTable;
        }

        private static string GetFormattedStringValue(ExporterConnection ExporterConnection, String valueString, object valueObject, IGridDataExportColumnInfo colInfo)
        {
            if (valueString == null)
            {
                valueString = String.Empty;
            }

            if (colInfo.Type == typeof(int) || colInfo.Type == typeof(Nullable<int>))
            {
                Int64 intProp;
                if (valueString.Length > 0)
                {
                    if (colInfo.ColumnType == GridDataExportColumnType.TableProperty)
                    {
                        if (Int64.TryParse(valueString, NumberStyles.Any, CultureInfo.InvariantCulture, out intProp)) valueString = intProp.ToString(CultureInfo.InvariantCulture);
                        else valueString = null;
                    }
                    else
                    {
                        if (Int64.TryParse(valueString, NumberStyles.Any, CultureInfo.CurrentCulture, out intProp)) valueString = intProp.ToString(CultureInfo.CurrentCulture);
                        else valueString = null;
                    }
                }
                else
                {
                    valueString = null;
                }
            }
            else if (colInfo.Type == typeof(double) || colInfo.Type == typeof(Nullable<double>))
            {
                if (valueObject is double valueDouble)
                    if (Double.IsPositiveInfinity(valueDouble))
                        valueString = "Infinity";
                    else if (Double.IsNegativeInfinity(valueDouble))
                        valueString = "-Infinity";
                    else if (Double.IsNaN(valueDouble))
                        valueString = null;
                    else
                        valueString = String.Format(CultureInfo.InvariantCulture, "{0}", valueDouble);
                else
                {
                    if (valueString.Length > 0)
                    {
                        double doubleProp;
                        if (colInfo.ColumnType == GridDataExportColumnType.TableProperty)
                        {
                            if (Double.TryParse(valueString, NumberStyles.Any, CultureInfo.InvariantCulture, out doubleProp)) valueString = doubleProp.ToString(CultureInfo.InvariantCulture);
                            else valueString = null;
                        }
                        else
                        {
                            if (Double.TryParse(valueString, NumberStyles.Any, CultureInfo.CurrentCulture, out doubleProp)) valueString = doubleProp.ToString(CultureInfo.CurrentCulture);
                            else valueString = null;
                        }
                    }
                    else                    
                        valueString = null;
                }
            }               
            else if (colInfo.Type == typeof(DateTime) || colInfo.Type == typeof(Nullable<DateTime>))
            {
                if (valueString.Length > 0)
                {
                    DateTime dateProp;
                    if (DateTime.TryParse(valueString, out dateProp)) valueString = dateProp.ToString(ExporterConnection.DateTimeFormat);
                    else valueString = null;
                }
                else
                {
                    valueString = null;
                }
            }

            return valueString;
        }

        private static string GetExportDataColumnType(ExporterConnection ExporterConnection, IGridDataExportColumnInfo col)
        {
            if (col.Type == typeof(double) || col.Type == typeof(Nullable<double>))
            {
                return "REAL";
            }
            else if (col.Type == typeof(int) || col.Type == typeof(Nullable<int>))
            {
                return "INTEGER";
            }
            else
            {
                return "TEXT";
            }
        }
    }

    internal class ExporterConnection : IDisposable
    {
        public ExporterConnection(string connectionString, Int32 connectionTimeout, string dateTimeFormat)
        {
            ConnectionTimeOut = connectionTimeout;
            DateTimeFormat = dateTimeFormat;
            Connection = new SQLiteConnection(connectionString);
            Connection.ConnectionString = connectionString;
            Connection.Open();
        }

        public SQLiteConnection Connection { get; }
        public Int32 ConnectionTimeOut { get; } = 600;
        public string DateTimeFormat { get; }

        public void Dispose()
        {
            if (Connection != null)
            {
                Connection.Close();
                Connection.Dispose();
            }
        }
    }
}
