using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Reflection;
using System.Threading.Tasks;

namespace NatWallbank.DatabaseUtilities
{
    /// <summary>
    /// Class providing abstractions over database query methods.
    /// </summary>
    public class DatabaseCommand
    {
        #region -- Properties --

        /// <summary>
        /// Gets or sets the InvariantName of the data provider to use in conjunction with the connection string to connect to the database.
        /// </summary>
        public string ProviderName { get; set; }

        /// <summary>
        /// Gets or sets the name of the connection string to use.
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the command text to execute.
        /// </summary>
        public string CommandText { get; set; }

        /// <summary>
        /// Gets or sets the type of command represented by CommandText.
        /// </summary>
        public CommandType Type { get; set; }

        /// <summary>
        /// Gets or sets the command timeout in seconds.
        /// </summary>
        public int CommandTimeout { get; set; }

        /// <summary>
        /// Gets or sets the parameters to pass to the command.
        /// </summary>
        /// <remarks>This should be an anonymous type with properties name in line with the expected parameters.</remarks>
        public dynamic Parameters { get; set; }

        #endregion

        #region -- Constructors --

        /// <summary>
        /// Initialises a new DatabaseCommand with command information.
        /// </summary>
        public DatabaseCommand(string providerName, string connectionString, CommandType type = CommandType.StoredProcedure)
        {
            if(providerName == null)
                throw new ArgumentNullException(nameof(providerName));
            if (connectionString == null)
                throw new ArgumentNullException(nameof(connectionString));

            ProviderName = providerName;
            ConnectionString = connectionString;
            Type = type;
            CommandTimeout = 30;
        }

        /// <summary>
        /// Initialises a new DatabaseCommand with command information.
        /// </summary>
        public DatabaseCommand(string providerName, string connectionString, string commandText, CommandType type = CommandType.StoredProcedure)
            : this(providerName, connectionString, type)
        {
            CommandText = commandText;
        }

        /// <summary>
        /// Initialises a new DatabaseCommand with command information.
        /// </summary>
        public DatabaseCommand(string providerName, string connectionString, string commandText, dynamic parameters, CommandType type = CommandType.StoredProcedure)
            : this(providerName, connectionString, commandText, type)
        {
            Parameters = parameters;
        }

        #endregion

        #region -- ExecuteNonQuery --

        /// <summary>
        /// Execute a command that does not return a result.
        /// </summary>
        public int ExecuteNonQuery()
        {
            using (var connection = GetConnection())
            {
                using (var cmd = SetupDbCommand(connection))
                {
                    connection.Open();
                    var rows = cmd.ExecuteNonQuery();
                    ParseOutputParameters(cmd);
                    return rows;
                }
            }
        }

        /// <summary>
        /// Asynchronously execute a command that does not return a result.
        /// </summary>
        public async Task<int> ExecuteNonQueryAsync()
        {
            using (var connection = GetConnection())
            {
                using (var cmd = SetupDbCommand(connection))
                {
                    await connection.OpenAsync();
                    var rows = await cmd.ExecuteNonQueryAsync();
                    ParseOutputParameters(cmd);
                    return rows;
                }
            }
        }

        #endregion

        #region -- GetScalar --

        /// <summary>
        /// Execute the command and return a single object derived from the first column of the first row of the record set.
        /// </summary>
        /// <typeparam name="TRecordType">The type of object that must be returned.</typeparam>
        /// <returns>The object returned.</returns>
        public TRecordType GetScalar<TRecordType>()
        {
            using (var connection = GetConnection())
            {
                using (var cmd = SetupDbCommand(connection))
                {
                    connection.Open();
                    var result = cmd.ExecuteScalar();
                    ParseOutputParameters(cmd);

                    if (result is TRecordType)
                        return (TRecordType)result;
                    return default(TRecordType);
                }
            }
        }

        /// <summary>
        /// Asynchronously execute the command and return a single object derived from the first column of the first row of the record set.
        /// </summary>
        /// <typeparam name="TRecordType">The type of object that must be returned.</typeparam>
        /// <returns>The object returned.</returns>
        public async Task<TRecordType> GetScalarAsync<TRecordType>()
        {
            using (var connection = GetConnection())
            {
                using (var cmd = SetupDbCommand(connection))
                {
                    await connection.OpenAsync();
                    var result = await cmd.ExecuteScalarAsync();
                    ParseOutputParameters(cmd);

                    if (result is TRecordType)
                        return (TRecordType)result;
                    return default(TRecordType);
                }
            }
        }

        /// <summary>
        /// Execute the command and return a single object derived from the specified column of the first row of the record set.
        /// </summary>
        /// <typeparam name="TRecordType">The type of object that must be returned.</typeparam>
        /// <param name="columnIndex">The index of the column from which to read data.</param>
        /// <returns>The object returned.</returns>
        public TRecordType GetScalar<TRecordType>(int columnIndex)
        {
            var result = default(TRecordType);

            ExecuteReader(reader =>
            {
                if (reader != null && reader.Read())
                    result = (TRecordType)reader[columnIndex];
            });

            return result;
        }

        /// <summary>
        /// Asynchronously execute the command and return a single object derived from the specified column of the first row of the record set.
        /// </summary>
        /// <typeparam name="TRecordType">The type of object that must be returned.</typeparam>
        /// <param name="columnIndex">The index of the column from which to read data.</param>
        /// <returns>The object returned.</returns>
        public async Task<TRecordType> GetScalarAsync<TRecordType>(int columnIndex)
        {
            var result = default(TRecordType);

            await ExecuteReaderAsync(async reader =>
            {
                if (reader != null && await reader.ReadAsync())
                    result = (TRecordType)reader[columnIndex];
            });

            return result;
        }

        /// <summary>
        /// Execute the command and return a single object derived from the specified column of the first row of the record set.
        /// </summary>
        /// <typeparam name="TRecordType">The type of object that must be returned.</typeparam>
        /// <param name="columnName">The name of the column from which to read data.</param>
        /// <returns>The object returned.</returns>
        public TRecordType GetScalar<TRecordType>(string columnName)
        {
            var result = default(TRecordType);

            ExecuteReader(reader =>
            {
                if (reader != null && reader.Read())
                    result = (TRecordType)reader[columnName];
            });

            return result;
        }

        /// <summary>
        /// Asynchronously execute the command and return a single object derived from the specified column of the first row of the record set.
        /// </summary>
        /// <typeparam name="TRecordType">The type of object that must be returned.</typeparam>
        /// <param name="columnName">The name of the column from which to read data.</param>
        /// <returns>The object returned.</returns>
        public async Task<TRecordType> GetScalarAsync<TRecordType>(string columnName)
        {
            var result = default(TRecordType);

            await ExecuteReaderAsync(async reader =>
            {
                if (reader != null && await reader.ReadAsync())
                    result = (TRecordType)reader[columnName];
            });

            return result;
        }

        #endregion

        #region -- GetVector --

        /// <summary>
        /// Execute the command and return a list of objects derived from the first column of each row of the record set.
        /// </summary>
        /// <typeparam name="TRecordType">The type of object that must be returned.</typeparam>
        /// <returns>The list of objects returned.</returns>
        public IEnumerable<TRecordType> GetVector<TRecordType>()
        {
            var items = new List<TRecordType>();

            ExecuteReader(reader =>
            {
                if (reader == null)
                    return;
                while (reader.Read())
                    items.Add((TRecordType)reader[0]);
            });

            return items;
        }

        /// <summary>
        /// Asynchronously execute the command and return a list of objects derived from the first column of each row of the record set.
        /// </summary>
        /// <typeparam name="TRecordType">The type of object that must be returned.</typeparam>
        /// <returns>The list of objects returned.</returns>
        public async Task<IEnumerable<TRecordType>> GetVectorAsync<TRecordType>()
        {
            var items = new List<TRecordType>();

            await ExecuteReaderAsync(async reader =>
            {
                if (reader == null)
                    return;
                while (await reader.ReadAsync())
                    items.Add((TRecordType)reader[0]);
            });

            return items;
        }

        /// <summary>
        /// Execute the command and return a list of objects derived from the specified column of each row of the record set.
        /// </summary>
        /// <typeparam name="TRecordType">The type of object that must be returned.</typeparam>
        /// <param name="columnIndex">The index of the column from which to read data.</param>
        /// <returns>The object returned.</returns>
        public IEnumerable<TRecordType> GetVector<TRecordType>(int columnIndex)
        {
            var items = new List<TRecordType>();

            ExecuteReader(reader =>
            {
                if (reader == null)
                    return;
                while (reader.Read())
                    items.Add((TRecordType)reader[columnIndex]);
            });

            return items;
        }

        /// <summary>
        /// Asynchronously execute the command and return a list of objects derived from the specified column of each row of the record set.
        /// </summary>
        /// <typeparam name="TRecordType">The type of object that must be returned.</typeparam>
        /// <param name="columnIndex">The index of the column from which to read data.</param>
        /// <returns>The object returned.</returns>
        public async Task<IEnumerable<TRecordType>> GetVectorAsync<TRecordType>(int columnIndex)
        {
            var items = new List<TRecordType>();

            await ExecuteReaderAsync(async reader =>
            {
                if (reader == null)
                    return;
                while (await reader.ReadAsync())
                    items.Add((TRecordType)reader[columnIndex]);
            });

            return items;
        }

        /// <summary>
        /// Execute the command and return a list of objects derived from the named column of each row of the record set.
        /// </summary>
        /// <typeparam name="TRecordType">The type of object that must be returned.</typeparam>
        /// <param name="columnName">The name of the column from which to read data.</param>
        /// <returns>The object returned.</returns>
        public IEnumerable<TRecordType> GetVector<TRecordType>(string columnName)
        {
            var items = new List<TRecordType>();

            ExecuteReader(reader =>
            {
                if (reader == null)
                    return;
                while (reader.Read())
                    items.Add((TRecordType)reader[columnName]);
            });

            return items;
        }

        /// <summary>
        /// Asynchronously execute the command and return a list of objects derived from the named column of each row of the record set.
        /// </summary>
        /// <typeparam name="TRecordType">The type of object that must be returned.</typeparam>
        /// <param name="columnName">The name of the column from which to read data.</param>
        /// <returns>The object returned.</returns>
        public async Task<IEnumerable<TRecordType>> GetVectorAsync<TRecordType>(string columnName)
        {
            var items = new List<TRecordType>();

            await ExecuteReaderAsync(async reader =>
            {
                if (reader == null)
                    return;
                while (await reader.ReadAsync())
                    items.Add((TRecordType)reader[columnName]);
            });

            return items;
        }

        #endregion

        #region -- GetRecord --

        /// <summary>
        /// Execute the command and return a single object derived from the first row of the record set.
        /// </summary>
        /// <typeparam name="TRecordType">The type of record that must be returned.</typeparam>
        /// <param name="fnRecordReader">Function that accepts a <see cref="DbDataReader"/> and produces a record of type <typeparamref name="TRecordType"/>.</param>
        /// <returns>The record returned.</returns>
        public TRecordType GetRecord<TRecordType>(Func<DbDataReader, TRecordType> fnRecordReader)
        {
            var result = default(TRecordType);

            ExecuteReader(reader =>
            {
                if (reader != null && reader.Read())
                    result = fnRecordReader(reader);
            });

            return result;
        }

        /// <summary>
        /// Asynchronously execute the command and return a single object derived from the first row of the record set.
        /// </summary>
        /// <typeparam name="TRecordType">The type of record that must be returned.</typeparam>
        /// <param name="fnRecordReader">Function that accepts a <see cref="DbDataReader"/> and produces a record of type <typeparamref name="TRecordType"/>.</param>
        /// <returns>The record returned.</returns>
        public async Task<TRecordType> GetRecordAsync<TRecordType>(Func<DbDataReader, TRecordType> fnRecordReader)
        {
            var result = default(TRecordType);

            await ExecuteReaderAsync(async reader =>
            {
                if (reader != null && await reader.ReadAsync())
                    result = fnRecordReader(reader);
            });

            return result;
        }

        #endregion

        #region -- GetRecords --

        /// <summary>
        /// Execute the command and return a set of objects derived from the record set.
        /// </summary>
        /// <param name="fnRecordReader">Function that accepts a <see cref="DbDataReader"/> and produces a record of type <typeparamref name="TRecordType"/>.</param>
        /// <typeparam name="TRecordType">The type of record that must be returned.</typeparam>
        /// <returns>The set of records returned.</returns>
        public IEnumerable<TRecordType> GetRecords<TRecordType>(Func<DbDataReader, TRecordType> fnRecordReader)
        {
            var records = new List<TRecordType>();

            ExecuteReader(reader =>
            {
                if (reader == null)
                    return;
                while (reader.Read())
                    records.Add(fnRecordReader(reader));
            });

            return records;
        }

        /// <summary>
        /// Asynchronously execute the command and return a set of objects derived from the record set.
        /// </summary>
        /// <param name="fnRecordReader">Function that accepts a <see cref="DbDataReader"/> and produces a record of type <typeparamref name="TRecordType"/>.</param>
        /// <typeparam name="TRecordType">The type of record that must be returned.</typeparam>
        /// <returns>The set of records returned.</returns>
        public async Task<IEnumerable<TRecordType>> GetRecordsAsync<TRecordType>(Func<DbDataReader, TRecordType> fnRecordReader)
        {
            var records = new List<TRecordType>();

            await ExecuteReaderAsync(async reader =>
            {
                if (reader == null)
                    return;
                while (await reader.ReadAsync())
                    records.Add(fnRecordReader(reader));
            });

            return records;
        }

        #endregion

        #region -- GetRecords --

        /// <summary>
        /// Execute the command and return a custom result derived from the record set.
        /// </summary>
        /// <param name="fnRecordReader">Function that accepts a <see cref="DbDataReader"/> and produces a record of type <typeparamref name="TResultType"/>.</param>
        /// <typeparam name="TResultType">The type of result that must be returned.</typeparam>
        /// <returns>The result of the custom result processor.</returns>
        public TResultType GetCustomResult<TResultType>(Func<DbDataReader, TResultType> fnRecordReader)
        {
            var oResult = default(TResultType);

            ExecuteReader(oReader =>
            {
                if (oReader == null)
                    return;
                oResult = fnRecordReader(oReader);
            });

            return oResult;
        }


        /// <summary>
        /// Asynchronously execute the command and return a custom result derived from the record set.
        /// </summary>
        /// <param name="fnRecordReader">Function that accepts a <see cref="DbDataReader"/> and produces a record of type <typeparamref name="TResultType"/>.</param>
        /// <typeparam name="TResultType">The type of result that must be returned.</typeparam>
        /// <returns>The result of the custom result processor.</returns>
        public async Task<TResultType> GetCustomResultAsync<TResultType>(Func<DbDataReader, TResultType> fnRecordReader)
        {
            var oResult = default(TResultType);

            await ExecuteReaderAsync(async oReader =>
            {
                if (oReader == null)
                    return;
                oResult = await Task.FromResult(fnRecordReader(oReader));
            });

            return oResult;
        }

        #endregion

        #region -- Key/Value Pairs --

        /// <summary>
        /// Build a dictionary from the first table of results of the query.
        /// </summary>
        /// <returns>A dictionary containing the query results.</returns>
        public IDictionary<TKey, TValue> GetKeyValuePairs<TKey, TValue>()
        {
            var dictionary = new Dictionary<TKey, TValue>();

            ExecuteReader(reader =>
            {
                if (reader == null)
                    return;
                while (reader.Read())
                    dictionary.Add((TKey)reader[0], (TValue)reader[1]);
            });

            return dictionary;
        }

        /// <summary>
        /// Build a dictionary from the first table of results of the query.
        /// </summary>
        /// <returns>A dictionary containing the query results.</returns>
        public async Task<IDictionary<TKey, TValue>> GetKeyValuePairsAsync<TKey, TValue>()
        {
            var dictionary = new Dictionary<TKey, TValue>();

            await ExecuteReaderAsync(async reader =>
            {
                if (reader == null)
                    return;
                while (await reader.ReadAsync())
                    dictionary.Add((TKey)reader[0], (TValue)reader[1]);
            });

            return dictionary;
        }

        #endregion

        #region -- DataTables and DataSets --

        /// <summary>
        /// Builds a data table from the first table of results of the query.
        /// </summary>
        /// <param name="name">An optional name to assign to the data table.</param>
        /// <returns>A DataTable containing the query results.</returns>
        public DataTable GetDataTable(string name = null)
        {
            var table = name == null ? new DataTable() : new DataTable(name);
            ExecuteReader(reader =>
            {
                table = BuildDataTableFromDataReader(reader);
            });
            return table;
        }

        /// <summary>
        /// Asynchronously builds a data table from the first table of results of the query.
        /// </summary>
        /// <param name="name">An optional name to assign to the data table.</param>
        /// <returns>A DataTable containing the query results.</returns>
        public async Task<DataTable> GetDataTableAsync(string name = null)
        {
            var table = new DataTable();
            await ExecuteReaderAsync(async reader =>
            {
                table = await BuildDataTableFromDataReaderAsync(reader);
            });

            table.TableName = name;
            return table;
        }

        /// <summary>
        /// Build a data set from all of the tables returned from a query.
        /// </summary>
        /// <returns>A DataSet containing all of the results of the query.</returns>
        public DataSet GetDataSet()
        {
            DataSet dataSet = null;
            ExecuteReader(reader =>
            {
                dataSet = BuildDataSetFromDataReader(reader);
            });
            return dataSet;
        }

        /// <summary>
        /// Asynchronously build a data set from all of the tables returned from a query.
        /// </summary>
        /// <returns>A DataSet containing all of the results of the query.</returns>
        public async Task<DataSet> GetDataSetAsync()
        {
            DataSet dataSet = null;
            await ExecuteReaderAsync(async reader =>
            {
                dataSet = await BuildDataSetFromDataReaderAsync(reader);
            });
            return dataSet;
        }

        /// <summary>
        /// Build a DataTable from the results in an DbDataReader.
        /// </summary>
        /// <param name="reader">The reader from which to build the table.</param>
        /// <returns>The table built from the results.</returns>
        public static DataTable BuildDataTableFromDataReader(DbDataReader reader)
        {
            var table = new DataTable();
            for (var nColumnIndex = 0; nColumnIndex < reader.FieldCount; nColumnIndex++)
            {
                var columnType = reader.GetFieldType(nColumnIndex);
                if (columnType != null)
                    table.Columns.Add(reader.GetName(nColumnIndex), columnType);
                else
                    table.Columns.Add(reader.GetName(nColumnIndex));
            }
            while (reader.Read())
            {
                var rowValues = new object[reader.FieldCount];
                reader.GetValues(rowValues);
                table.Rows.Add(rowValues);
            }

            return table;
        }

        /// <summary>
        /// Asynchronously build a DataTable from the results in an DbDataReader.
        /// </summary>
        /// <param name="reader">The reader from which to build the table.</param>
        /// <returns>The table built from the results.</returns>
        public static async Task<DataTable> BuildDataTableFromDataReaderAsync(DbDataReader reader)
        {
            var table = new DataTable();
            for (var nColumnIndex = 0; nColumnIndex < reader.FieldCount; nColumnIndex++)
            {
                var columnType = reader.GetFieldType(nColumnIndex);
                if (columnType != null)
                    table.Columns.Add(reader.GetName(nColumnIndex), columnType);
                else
                    table.Columns.Add(reader.GetName(nColumnIndex));
            }

            while (await reader.ReadAsync())
            {
                var rowValues = new object[reader.FieldCount];
                reader.GetValues(rowValues);
                table.Rows.Add(rowValues);
            }

            return table;
        }

        /// <summary>
        /// Builds a DataSet from the results in a DbDataReader.
        /// </summary>
        /// <param name="reader">The reader from which to build the data set.</param>
        /// <returns>The data set built from the results.</returns>
        public static DataSet BuildDataSetFromDataReader(DbDataReader reader)
        {
            var dataSet = new DataSet();
            do
            {
                var table = BuildDataTableFromDataReader(reader);
                table.TableName = dataSet.Tables.Count.ToString(CultureInfo.InvariantCulture);
                dataSet.Tables.Add(table);
            }
            while (reader.NextResult());

            // If the first table contains a single column called 'TableName', remove it
            // from the data set and assign the table names from the rows to each item.
            if ((dataSet.Tables.Count > 1) &&
                (dataSet.Tables[0].Columns.Count == 1) &&
                (dataSet.Tables[0].Columns[0].ColumnName != null) &&
                (dataSet.Tables[0].Columns[0].ColumnName.Equals("TableName")))
            {
                for (var nTableIndex = 0; (nTableIndex < dataSet.Tables.Count - 1) && (nTableIndex < dataSet.Tables[0].Rows.Count); nTableIndex++)
                    dataSet.Tables[nTableIndex + 1].TableName = (string)dataSet.Tables[0].Rows[nTableIndex]["TableName"];
                dataSet.Tables.RemoveAt(0);
            }
            return dataSet;
        }

        /// <summary>
        /// Asynchronously builds a DataSet from the results in a DbDataReader.
        /// </summary>
        /// <param name="reader">The reader from which to build the data set.</param>
        /// <returns>The data set built from the results.</returns>
        public static async Task<DataSet> BuildDataSetFromDataReaderAsync(DbDataReader reader)
        {
            var dataSet = new DataSet();
            do
            {
                var table = await BuildDataTableFromDataReaderAsync(reader);
                table.TableName = dataSet.Tables.Count.ToString(CultureInfo.InvariantCulture);
                dataSet.Tables.Add(table);
            }
            while (await reader.NextResultAsync());

            // If the first table contains a single column called 'TableName', remove it
            // from the data set and assign the table names from the rows to each item.
            if ((dataSet.Tables.Count > 1) &&
                (dataSet.Tables[0].Columns.Count == 1) &&
                (dataSet.Tables[0].Columns[0].ColumnName != null) &&
                (dataSet.Tables[0].Columns[0].ColumnName.Equals("TableName")))
            {
                for (var nTableIndex = 0; (nTableIndex < dataSet.Tables.Count - 1) && (nTableIndex < dataSet.Tables[0].Rows.Count); nTableIndex++)
                    dataSet.Tables[nTableIndex + 1].TableName = (string)dataSet.Tables[0].Rows[nTableIndex]["TableName"];
                dataSet.Tables.RemoveAt(0);
            }
            return dataSet;
        }

        #endregion

        #region -- Command Helpers --

        /// <summary>
        /// Initialises n DbCommand object configured according to this DatabaseCommand.
        /// </summary>
        /// <param name="connection">The connection to use with the command.</param>
        /// <returns>A DbCommand object, ready to execute a query.</returns>
        private DbCommand SetupDbCommand(DbConnection connection)
        {
            var command = connection.CreateCommand();
            command.CommandType = Type;
            command.CommandText = CommandText;
            command.CommandTimeout = CommandTimeout;
            SetupDbCommandParameters(command);
            return command;
        }

        /// <summary>
        /// Configures the collection of parameters for a <see cref="DbCommand"/> instance.
        /// </summary>
        /// <param name="command">The command object to configure the parameters for.</param>
        internal void SetupDbCommandParameters(DbCommand command)
        {
            if (Parameters == null)
                return;

            Type type = Parameters.GetType();
            foreach (var prop in type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.GetProperty))
            {
                var getMethod = prop.GetGetMethod();
                if (getMethod == null || getMethod.GetParameters().Length > 0)
                    continue;

                var name = prop.Name;
                var value = prop.GetValue(Parameters);
                var paramType = prop.PropertyType;

                var param = command.CreateParameter();
                param.ParameterName = name;

                if (value == null)
                {
                    // explicitly set to NULL so that we don't require optional parameters in sproc
                    param.Value = DBNull.Value;
                }
                else if (value is byte[])
                {
                    param.Value = value;
                }
                else if (value is IList)
                {
                    // table-valued parameter
                    param.Value = GetDataTableParameter((IList)value);
                }
                else if (paramType == typeof(DatabaseParameter))
                {
                    var detailedParam = (DatabaseParameter)value;
                    param.ParameterName = string.IsNullOrEmpty(detailedParam.Name) ? name : detailedParam.Name;
                    param.Direction = detailedParam.Direction;
                    param.DbType = detailedParam.DbType ?? param.DbType;
                    param.Size = detailedParam.Size ?? param.Size;
                    param.Value = detailedParam.Value;
                }
                else
                    param.Value = value;

                command.Parameters.Add(param);
            }
        }

        /// <summary>
        /// Converts a list of objects to a data table for use as a table-valued parameter.
        /// </summary>
        /// <remarks>IMPORTANT: list must be homogeneous or this won't work!</remarks>
        /// <param name="parameter">The list of objects to convert to a data table.</param>
        /// <returns>A data table representing the list of supplied objects.</returns>
        internal DataTable GetDataTableParameter(IList parameter)
        {
            var table = new DataTable();
            if (parameter.Count == 0)
                return null;

            // initialise columns
            // assumes list is homogeneous
            var type = parameter[0].GetType();
            var props = new List<PropertyInfo>();
            foreach (var prop in type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.GetProperty))
            {
                var getMethod = prop.GetGetMethod();
                if (getMethod == null || getMethod.GetParameters().Length > 0)
                    continue;

                props.Add(prop);
                table.Columns.Add(prop.Name, prop.PropertyType);
            }
            props.TrimExcess();

            // now loop through each actual row and create in data table
            foreach (var obj in parameter)
            {
                var row = new object[props.Count];
                for (var i = 0; i < props.Count; i++)
                    row[i] = props[i].GetValue(obj);
                table.Rows.Add(row);
            }

            return table;
        }

        /// <summary>
        /// Parses any configured output parameters and stores the result.
        /// </summary>
        /// <param name="cmd">The DbCommand from which to retrieve the parameters.</param>
        internal void ParseOutputParameters(DbCommand cmd)
        {
            if (Parameters == null)
                return;

            Type type = Parameters.GetType();
            foreach (
                var prop in type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.GetProperty))
            {
                var getMethod = prop.GetGetMethod();
                if (getMethod == null || getMethod.GetParameters().Length > 0)
                    continue;

                var name = prop.Name;
                var value = prop.GetValue(Parameters);
                var paramType = getMethod.ReturnType;

                if (paramType != typeof(DatabaseParameter))
                    continue;

                var detailedParam = (DatabaseParameter)value;
                if (string.IsNullOrEmpty(detailedParam.Name))
                    detailedParam.Name = name;
                if (detailedParam.Direction == ParameterDirection.Output && cmd.Parameters.Contains(detailedParam.Name))
                {
                    var dbParam = cmd.Parameters[detailedParam.Name];
                    detailedParam.Value = dbParam.Value;
                }
            }
        }

        #endregion

        #region -- DbConnection Helpers --

        /// <summary>
        /// Instantiates a database connection.
        /// </summary>
        /// <returns></returns>
        private DbConnection GetConnection()
        {
            DbConnection cnn;

            try
            {
                var factory = DbProviderFactories.GetFactory(ProviderName);
                cnn = factory.CreateConnection();
                if (cnn != null)
                    cnn.ConnectionString = ConnectionString;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error creating connection for Provider: {ProviderName} <{ConnectionString}>: {ex.Message}");
                throw;
            }

            return cnn;
        }

        #endregion

        #region -- DbCommand Helpers --

        /// <summary>
        /// Initialises, executes and tidies up after a reader.
        /// </summary>
        /// <param name="processReader">Action to allow the caller to process the reader.</param>
        private void ExecuteReader(Action<DbDataReader> processReader)
        {
            using (var connection = GetConnection())
            {
                using (var cmd = SetupDbCommand(connection))
                {
                    connection.Open();
                    var reader = cmd.ExecuteReader();
                    ParseOutputParameters(cmd);
                    processReader(reader);
                }
            }
        }

        /// <summary>
        /// Initialises, executes and tidies up after a reader asynchronously.
        /// </summary>
        /// <param name="processReader">Action to allow the caller to process the reader.</param>
        private async Task ExecuteReaderAsync(Func<DbDataReader, Task> processReader)
        {
            using (var connection = GetConnection())
            {
                using (var cmd = SetupDbCommand(connection))
                {
                    await connection.OpenAsync();
                    var reader = await cmd.ExecuteReaderAsync();
                    ParseOutputParameters(cmd);
                    await processReader(reader);
                }
            }
        }

        #endregion
    }
}
