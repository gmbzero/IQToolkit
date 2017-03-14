using IQToolkit.Data.Common;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;

namespace IQToolkit.Data
{
    internal partial class DbEntityProvider
    {
        public partial class DbQueryExecutor : QueryExecutor
        {
            private readonly DbEntityProvider provider;
            private int rowsAffected;

            public DbEntityProvider Provider
            {
                get { return this.provider; }
            }

            public override int RowsAffected
            {
                get { return this.rowsAffected; }
            }

            public virtual bool BufferResultRows
            {
                get { return false; }
            }

            public bool ActionOpenedConnection
            {
                get { return this.provider.actionOpenedConnection; }
            }

            public DbQueryExecutor(DbEntityProvider provider)
            {
                this.provider = provider;
            }

            public void StartUsingConnection()
            {
                this.provider.StartUsingConnection();
            }

            public void StopUsingConnection()
            {
                this.provider.StopUsingConnection();
            }

            public override object Convert(object value, Type type)
            {
                if (value == null)
                {
                    return TypeHelper.GetDefault(type);
                }

                type = TypeHelper.GetNonNullableType(type);

                var vtype = value.GetType();

                if (type != vtype)
                {
                    if (type.GetTypeInfo().IsEnum)
                    {
                        if (vtype == typeof(string))
                        {
                            return Enum.Parse(type, (string)value);
                        }

                        var utype = Enum.GetUnderlyingType(type);

                        if (utype != vtype)
                        {
                            value = System.Convert.ChangeType(value, utype);
                        }

                        return Enum.ToObject(type, value);
                    }

                    return System.Convert.ChangeType(value, type);
                }

                return value;
            }

            public override IEnumerable<T> Execute<T>(QueryCommand command, Func<FieldReader, T> fnProjector, MappingEntity entity, object[] paramValues)
            {
                var executable = new DbQueryExecutable<T>(this, command, paramValues, fnProjector);

                if (this.BufferResultRows)
                {
                    return new QueryResult<T>(new EnumerableBuffered<T>(executable));
                }
                else
                {
                    return new QueryResult<T>(executable);
                }
            }

            public override int ExecuteCommand(QueryCommand query, object[] paramValues)
            {
                this.StartUsingConnection();

                try
                {
                    this.LogCommand(query, paramValues);

                    var cmd = this.GetCommand(query, paramValues);

                    this.rowsAffected = cmd.ExecuteNonQuery();

                    return this.rowsAffected;
                }
                finally
                {
                    this.StopUsingConnection();
                }
            }

            public override IEnumerable<int> ExecuteBatch(QueryCommand query, IEnumerable<object[]> paramSets, int batchSize, bool stream)
            {
                var batch = new DbQueryCommandBatchExecutable(this, query, paramSets.ToArray());

                if (stream == false)
                {
                    return new EnumerableBuffered<int>(batch);
                }
                else
                {
                    return batch;
                }
            }

            public override IEnumerable<T> ExecuteBatch<T>(QueryCommand query, IEnumerable<object[]> paramSets, Func<FieldReader, T> fnProjector, MappingEntity entity, int batchSize, bool stream)
            {
                var batch = new DbQueryBatchExecutable<T>(this, query, paramSets.ToArray(), fnProjector);

                if (stream == false)
                {
                    return new EnumerableBuffered<T>(batch);
                }
                else
                {
                    return batch;
                }
            }

            public override IEnumerable<T> ExecuteDeferred<T>(QueryCommand query, Func<FieldReader, T> fnProjector, MappingEntity entity, object[] paramValues)
            {
                return new EnumerableBuffered<T>(new DbQueryExecutable<T>(this, query, paramValues, fnProjector));
            }

            public virtual DbCommand GetCommand(QueryCommand query, object[] paramValues = null)
            {
                var cmd = this.provider.Connection.CreateCommand();

                if (cmd != null)
                {
                    cmd.CommandText = query.CommandText;
                }

                if (this.provider.Transaction != null)
                {
                    cmd.Transaction = this.provider.Transaction;
                }

                this.SetParameterValues(query, cmd, paramValues);

                return cmd;
            }

            public virtual void SetParameterValues(QueryCommand query, DbCommand command, object[] paramValues)
            {
                if (query.Parameters.Count > 0 && command.Parameters.Count == 0)
                {
                    for (int i = 0, n = query.Parameters.Count; i < n; i++)
                    {
                        this.AddParameter(command, query.Parameters[i], paramValues?[i]);
                    }
                }
                else if (paramValues != null)
                {
                    for (int i = 0, n = command.Parameters.Count; i < n; i++)
                    {
                        var p = command.Parameters[i];

                        if (p.Direction == ParameterDirection.Input || p.Direction == ParameterDirection.InputOutput)
                        {
                            p.Value = paramValues[i] ?? DBNull.Value;
                        }
                    }
                }
            }

            protected virtual void AddParameter(DbCommand command, QueryParameter parameter, object value)
            {
                var p = command.CreateParameter();

                p.ParameterName = parameter.Name;
                p.Value = value ?? DBNull.Value;

                command.Parameters.Add(p);
            }

            public virtual void GetParameterValues(DbCommand command, object[] paramValues)
            {
                if (paramValues != null)
                {
                    for (int i = 0, n = command.Parameters.Count; i < n; i++)
                    {
                        if (command.Parameters[i].Direction != ParameterDirection.Input)
                        {
                            var value = command.Parameters[i].Value;

                            if (value == DBNull.Value)
                            {
                                value = null;
                            }

                            paramValues[i] = value;
                        }
                    }
                }
            }

            public virtual void LogMessage(string message)
            {
                if (this.provider.Log != null)
                {
                    this.provider.Log.WriteLine(message);
                }
            }

            public virtual void LogCommand(QueryCommand command, object[] parameters = null)
            {
                if (this.provider.Log != null)
                {
                    this.provider.Log.WriteLine(command.CommandText);

                    if (parameters != null)
                    {
                        this.LogParameters(command, parameters);
                    }

                    this.provider.Log.WriteLine();
                }
            }

            public virtual void LogParameters(QueryCommand command, object[] parameters)
            {
                if (this.provider.Log != null)
                {
                    for (int i = 0, n = command.Parameters.Count; i < n; i++)
                    {
                        var p = command.Parameters[i];
                        var v = parameters[i];

                        if (v == null || v == DBNull.Value)
                        {
                            this.provider.Log.WriteLine("-- {0} = NULL", p.Name);
                        }
                        else
                        {
                            this.provider.Log.WriteLine("-- {0} = [{1}]", p.Name, v);
                        }
                    }
                }
            }
        }
    }
}