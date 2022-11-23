using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Drawing;
using System.Text;
using System.Threading.Tasks;
using SimioAPI;
using SimioAPI.Extensions;
using System.Data.SQLite;

namespace SQLiteGridData
{
    public class ImporterDefinition : IGridDataImporterDefinition
    {
        public string Name => "SQLite Data Importer";
        public string Description => "An importer for SQLite";
        public Image Icon => null;

        static readonly Guid MY_ID = new Guid("5079ef25-6065-456b-884d-3b2af99af554");
        public Guid UniqueID => MY_ID;

        public IGridDataImporter CreateInstance(IGridDataImporterContext context)
        {
            return new Importer(context);
        }

        public void DefineSchema(IGridDataSchema schema)
        {
            var connectionStringProp = schema.OverallProperties.AddStringProperty("ConnectionString");
            connectionStringProp.DisplayName = "Connection String";
            connectionStringProp.Description = "Database Connection String.";
            connectionStringProp.DefaultValue = "URI=file:C:\\temp\\test.db";

            var connectionTimeOutProp = schema.OverallProperties.AddStringProperty("ConnectionTimeOut");
            connectionTimeOutProp.DisplayName = "Connection TimeOut (seconds)";
            connectionTimeOutProp.Description = "Connection TimeOut in seconds.";
            connectionTimeOutProp.DefaultValue = "30";

            var sqlStatementProp = schema.PerTableProperties.AddStringProperty("SQLStatement");
            sqlStatementProp.DisplayName = "SQL Statement";
            sqlStatementProp.Description = "The SQL statement to read from";
            sqlStatementProp.IsMultiLine = true;
            sqlStatementProp.DefaultValue = String.Empty;
        }
    }

    class Importer : IGridDataImporter
    {
        public Importer(IGridDataImporterContext context)
        {
        }

        public OpenImportDataResult OpenData(IGridDataOpenImportDataContext openContext)
        {
            GetValues(openContext.TableName, openContext.Settings, out var connectionString, out var connectionTimeOut, out var sqlStatement);


            if (String.IsNullOrWhiteSpace(connectionString))
                return OpenImportDataResult.Failed("The Connection String parameter is not specified");
            if ((ImporterConnection.GetConnectionString() != null && connectionString != ImporterConnection.GetConnectionString()))
            {
                ImporterConnection.CloseConnection();
            }
            ImporterConnection.SetConnectionString(connectionString);

            if (connectionTimeOut <= 0)
                return OpenImportDataResult.Failed("The Connection TimeOut parameter needs to be greater than zero");
            ImporterConnection.SetConnectionTimeOut(connectionTimeOut);

            if (String.IsNullOrWhiteSpace(sqlStatement))
                return OpenImportDataResult.Failed("The SQL Statement parameter is not specified");

            ImporterConnection.CheckConnection();

            return new OpenImportDataResult()
            {
                Result = GridDataOperationResult.Succeeded,
                Records = new SQLiteGridDataRecords(connectionString, connectionTimeOut, sqlStatement)
            };
        }

        public string GetDataSummary(IGridDataSummaryContext context)
        {
            if (context == null)
                return null;

            GetValues(context.GridDataName, context.Settings, out var connectionString, out _, out var sqlStatement);

            if (sqlStatement == null)
                return null;

            return String.Format("Bound to {0} : '{1}' statement", connectionString, sqlStatement);
        }

        private static void GetValues(string tableName, IGridDataOverallSettings settings, out string connectionString, out int connectionTimeOut, out string sqlStatement)
        {
            connectionString = (string)settings.Properties["ConnectionString"].Value;
            connectionTimeOut = Convert.ToInt32(settings.Properties["ConnectionTimeOut"].Value);
            sqlStatement = (string)settings.GridDataSettings[tableName]?.Properties["SQLStatement"]?.Value;            
        }

        public void Dispose()
        {
            ImporterConnection.CloseConnection();
        }
    }

    class SQLiteGridDataRecords : IGridDataRecords
    {
        readonly string _connectionString;
        readonly int _connectionTimeOut;
        readonly string _sqlStatement;

        // Acts as a cached dataset, specifically for cases of stored procedures, which may have side effects, so we only
        // want to 'call' once, not once to get schema, then AGAIN to get the data. We'll also use this for SQL 
        // statements... that really could be anything.
        DataSet _ds;

        public SQLiteGridDataRecords(string connectionString, int connectionTimeOut, string sqlStatement)
        {
            _connectionString = connectionString;
            _connectionTimeOut = connectionTimeOut;
            _sqlStatement = sqlStatement;
        }

        #region IGridDataRecords Members

        List<GridDataColumnInfo> _columnInfo;
        List<GridDataColumnInfo> ColumnInfo
        {
            get
            {
                if (_columnInfo == null)
                {
                    _columnInfo = new List<GridDataColumnInfo>();

                    IEnumerable<DbColumnInfo> dbColumnInfos;
                    if (_ds == null)
                        _ds = SQLiteGridDataUtils.GetDataSet(ImporterConnection.GetDbConnection(), _sqlStatement, _connectionTimeOut);

                    dbColumnInfos = SQLiteGridDataUtils.GetColumnInfoForTable(_ds);


                    foreach(var i in dbColumnInfos)
                    {
                        _columnInfo.Add(new GridDataColumnInfo { Name = i.Name, Type = i.Type });
                    }
                }

                return _columnInfo;
            }
        }

        public IEnumerable<GridDataColumnInfo> Columns
        {
            get { return ColumnInfo; }
        }

        #endregion

        #region IEnumerable<IGridDataRecord> Members

        public IEnumerator<IGridDataRecord> GetEnumerator()
        {
            if (_ds == null)
                _ds = SQLiteGridDataUtils.GetDataSet(ImporterConnection.GetDbConnection(), _sqlStatement, _connectionTimeOut);

            foreach (DataRow dr in _ds.Tables[0].Rows)
            {
                yield return new SQLiteGridDataRecord(dr, _ds.Tables[0].Columns.Count);
            }

            _ds.Clear();
            _ds = null;
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
        }

        #endregion
    }

    class SQLiteGridDataRecord : IGridDataRecord
    {
        private readonly DataRow _dr;
        public SQLiteGridDataRecord(DataRow dr, Int32 numberOfColumns)
        {
            _dr = dr;
        }

        #region IGridDataRecord Members

        public string this[int index]
        {
            get
            {
                var theValue = _dr[index];

                // Simio will first try to parse dates in the current culture
                if (theValue is DateTime)
                    return ((DateTime)theValue).ToString();

                return String.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}", _dr[index]);
            }
        }

        #endregion
    }

    internal static class ImporterConnection
    {
        private static SQLiteConnection _connection;
        private static Int32 _connectionTimeOut = 30;
        private static string _connectionString = string.Empty;
        private static string _dateTimeFormatString = string.Empty;

        internal static void SetConnectionTimeOut(Int32 connectionTimeOut)
        {
            _connectionTimeOut = connectionTimeOut;
        }

        internal static void SetConnectionString(string connectionString)
        {
            _connectionString = connectionString;
        }
        internal static string GetConnectionString()
        {
            return _connectionString;
        }

        internal static void SetDateTimeFormatString(string dateTimeFormatString)
        {
            _dateTimeFormatString = dateTimeFormatString;
        }

        internal static void SetConnection(string connectionString)
        {
            if (_connection == null)
            {               
                _connection = new SQLiteConnection();
                _connection.ConnectionString = connectionString;
            }
            if (_connection.State == ConnectionState.Closed)
            {
                _connection.Open();
            }
        }

        internal static SQLiteConnection GetDbConnection()
        {
            return _connection;
        }

        internal static string GetDateTimeFormat()
        {
            return _dateTimeFormatString;
        }

        internal static void CloseConnection()
        {
            if (_connection != null)
            {
                _connection.Close();
                _connection.Dispose();
                _connection = null;
            }
        }

        internal static void CheckConnection()
        {
            if (_connectionString.Length == 0)
            {
                throw new Exception("ConnectionString Is Blank");
            }
            else if (_connection == null)
            {
                SetConnection(_connectionString);
            }

            if (_connection.State == ConnectionState.Closed)
            {
                // if connection is closed, reset connection and try again.
                CloseConnection();
                SetConnection(_connectionString);
                if (_connection.State == ConnectionState.Closed)
                {
                    throw new Exception("Connection Is Closed.  Fix Connection String and Retry");
                }
            }
        }
    }
}
