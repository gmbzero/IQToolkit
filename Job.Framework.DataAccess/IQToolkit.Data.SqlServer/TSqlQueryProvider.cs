using IQToolkit.Data.Common;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;

namespace IQToolkit.Data.SqlServer
{
    internal class TSqlQueryProvider : DbEntityProvider
    {
        private bool? allowMulitpleActiveResultSets;

        public TSqlQueryProvider(DbConnection connection, QueryMapping mapping, QueryPolicy policy) : base(connection, TSqlLanguage.Default, mapping, policy)
        {
            this.allowMulitpleActiveResultSets = null;
        }

        public override DbEntityProvider New(DbConnection connection, QueryMapping mapping, QueryPolicy policy)
        {
            return new TSqlQueryProvider(connection, mapping, policy);
        }

        public bool AllowsMultipleActiveResultSets
        {
            get
            {
                if (this.allowMulitpleActiveResultSets == null)
                {
                    var builder = new DbConnectionStringBuilder
                    {
                        ConnectionString = this.Connection.ConnectionString
                    };

                    var result = builder["MultipleActiveResultSets"];

                    if (result != null && result.GetType() == typeof(bool) && (bool)result)
                    {
                        this.allowMulitpleActiveResultSets = true;
                    }
                    else
                    {
                        this.allowMulitpleActiveResultSets = false;
                    }
                }

                return this.allowMulitpleActiveResultSets.Value;
            }
        }

        protected override QueryExecutor CreateExecutor()
        {
            return new Executor(this);
        }

        private class Executor : DbQueryExecutor
        {
            private readonly TSqlQueryProvider provider;

            public Executor(TSqlQueryProvider provider) : base(provider)
            {
                this.provider = provider;
            }

            public override bool BufferResultRows
            {
                get { return this.provider.AllowsMultipleActiveResultSets == false; }
            }

            protected override void AddParameter(DbCommand command, QueryParameter parameter, object value)
            {
                var p = command.CreateParameter();

                p.ParameterName = "@" + parameter.Name;
                p.Value = value ?? DBNull.Value;

                command.Parameters.Add(p);
            }

            public override IEnumerable<int> ExecuteBatch(QueryCommand query, IEnumerable<object[]> paramSets, int batchSize, bool stream)
            {
                this.StartUsingConnection();

                try
                {
                    var result = this.ExecuteBatch(query, paramSets, batchSize);

                    if (stream == false || this.ActionOpenedConnection)
                    {
                        return result.ToList();
                    }
                    else
                    {
                        return new EnumerateOnce<int>(result);
                    }
                }
                finally
                {
                    this.StopUsingConnection();
                }
            }

            private IEnumerable<int> ExecuteBatch(QueryCommand query, IEnumerable<object[]> paramSets, int batchSize)
            {
                return null;

                //var cmd = this.GetCommand(query, null);
                //DataTable dataTable = new DataTable();

                //for (int i = 0, n = query.Parameters.Count; i < n; i++)
                //{
                //    var qp = query.Parameters[i];
                //    cmd.Parameters[i].SourceColumn = qp.Name;
                //    dataTable.Columns.Add(qp.Name, TypeHelper.GetNonNullableType(qp.Type));
                //}


                //SqlDataAdapter dataAdapter = new SqlDataAdapter();
                //dataAdapter.InsertCommand = cmd;
                //dataAdapter.InsertCommand.UpdatedRowSource = UpdateRowSource.None;
                //dataAdapter.UpdateBatchSize = batchSize;

                //this.LogMessage("-- Start SQL Batching --");
                //this.LogMessage("");
                //this.LogCommand(query);

                //IEnumerator<object[]> en = paramSets.GetEnumerator();
                //using (en)
                //{
                //    bool hasNext = true;
                //    while (hasNext)
                //    {
                //        int count = 0;
                //        for (; count < dataAdapter.UpdateBatchSize && (hasNext = en.MoveNext()); count++)
                //        {
                //            var paramValues = en.Current;
                //            dataTable.Rows.Add(paramValues);
                //            this.LogParameters(query, paramValues);
                //            this.LogMessage("");
                //        }

                //        if (count > 0)
                //        {
                //            int n = dataAdapter.Update(dataTable);
                //            for (int i = 0; i < count; i++)
                //            {
                //                yield return (i < n) ? 1 : 0;
                //            }

                //            dataTable.Rows.Clear();
                //        }
                //    }
                //}

                //this.LogMessage(string.Format("-- End SQL Batching --"));
                //this.LogMessage("");
            }
        }
    }
}