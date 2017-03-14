using IQToolkit;
using IQToolkit.Data.Common;
using IQToolkit.Data.Mapping;
using IQToolkit.Data.SqlServer;
using Job.Framework.Common;
using System;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

namespace Job.Framework.DataAccess
{
    /// <summary>
    /// 提供一个数据库访问上下文
    /// </summary>
    public abstract class DbContextBase : IDisposable
    {
        #region 相关属性

        /// <summary>
        /// 获取数据库访问上下文构造者
        /// </summary>
        internal DbContextOptins Options { get; } = new DbContextOptins();

        #endregion

        /// <summary>
        /// 抽象一个方法，必须对数据库访问上下文进行配置
        /// </summary>
        /// <param name="options">数据库访问上下文配置</param>
        protected abstract void OnConfiguring(DbContextOptins options);

        protected abstract void OnCreateEntity(DbContextOptins options);
        
        #region 构造函数

        public DbContextBase()
        {
            this.OnConfiguring(Options);
            this.OnCreateEntity(Options);
        }

        #endregion

        #region 相关方法


        /// <summary>
        /// 创建一个命令对象提供者
        /// </summary>
        /// <param name="cmdText">命令语句或者存储过程名称</param>
        /// <param name="cmdParams">命令参数集合</param>
        /// <param name="cmdType">如何解析命令字符串</param>
        /// <returns>返回一个命令对象提供者</returns>
        internal DbCommand CreateDbCommand(string cmdText, DbSqlParameter[] cmdParams, CommandType cmdType)
        {
            var commandTimeout = this.Options.CommandTimeout;
            var dbConnection = this.Options.DbConnection;
            var cmd = dbConnection.CreateCommand() as DbCommand;

            try
            {
                var trans = DbSqlTransaction.Get(dbConnection.ConnectionString);

                if (trans != null)
                {
                    cmd.Transaction = trans;
                    cmd.Connection = trans.Connection;
                }
                else
                {
                    if (cmd.Connection.State != ConnectionState.Open)
                    {
                        cmd.Connection.Open();
                    }

                    if (DbSqlTransaction.IsBeginTransaction)
                    {
                        DbSqlTransaction.Set(dbConnection.ConnectionString, cmd.Transaction = cmd.Connection.BeginTransaction());
                    }
                }

                cmd.CommandText = cmdText;
                cmd.CommandType = cmdType;

                if (commandTimeout > 0)
                {
                    cmd.CommandTimeout = commandTimeout;
                }

                if (cmdParams != null)
                {
                    foreach (var p in cmdParams)
                    {
                        var parameter = cmd.CreateParameter();

                        parameter.Value = p.Value ?? DBNull.Value;
                        parameter.ParameterName = p.ParameterName;
                        parameter.Direction = p.Direction;

                        cmd.Parameters.Add(parameter);
                    }
                }
            }
            catch (Exception ex)
            {
                cmd.Connection.Dispose();
                cmd.Dispose();

                throw ex;
            }

            return cmd;
        }

        #endregion

        #region 命令方法

        #region ExecuteScalar

        #region Text

        /// <summary>
        /// 异步执行命令语句，返回第一行第一列的值
        /// </summary>
        /// <param name="sql">命令语句</param>
        /// <param name="cmdParams">命令参数集合</param>
        /// <returns>返回第一行第一列的值</returns>
        public Task<object> ExecuteScalarAsync(string sql, DbSqlParameter[] cmdParams = null)
        {
            return ExecuteScalarAsync(sql, cmdParams, CommandType.Text);
        }

        #endregion

        #region StoredProcedure

        /// <summary>
        /// 异步调用存储过程，返回第一行第一列的值
        /// </summary>
        /// <param name="spName">存储过程名称</param>
        /// <param name="cmdParams">命令参数集合</param>
        /// <returns>返回第一行第一列的值</returns>
        public Task<object> ExecuteScalarSpAsync(string spName, DbSqlParameter[] cmdParams = null)
        {
            return ExecuteScalarAsync(spName, cmdParams, CommandType.StoredProcedure);
        }

        #endregion

        #region Common

        /// <summary>
        /// 异步执行数据库语句或者调用存储过程，返回第一行第一列的值
        /// </summary>
        /// <param name="objText">命令语句或者存储过程名称</param>
        /// <param name="cmdParams">命令参数集合</param>
        /// <param name="cmdType">如何解析命令字符串</param>
        /// <returns>返回第一行第一列的值</returns>
        private Task<object> ExecuteScalarAsync(string objText, DbSqlParameter[] cmdParams, CommandType cmdType)
        {
            using (var dbCommand = CreateDbCommand(objText, cmdParams, cmdType))
            {
                try
                {
                    return dbCommand.ExecuteScalarAsync();
                }
                catch (Exception ex) { throw ex; }
                finally
                {
                    dbCommand.DisposedConnection();
                }
            }
        }

        #endregion

        #endregion

        #region ExecuteReader

        #region Text

        /// <summary>
        /// 异步执行命令语句，返回 DbDataReader 只进流
        /// </summary>
        /// <param name="sql">命令语句</param>
        /// <param name="cmdParams">命令参数集合</param>
        /// <returns>返回 DbDataReader 只进流</returns>
        public Task<DbDataReader> ExecuteReaderAsync(string sql, DbSqlParameter[] cmdParams = null)
        {
            return ExecuteReaderAsync(sql, cmdParams, CommandType.Text);
        }

        #endregion

        #region StoredProcedure

        /// <summary>
        /// 异步调用存储过程，返回 DbDataReader 只进流
        /// </summary>
        /// <param name="spName">存储过程名称</param>
        /// <param name="cmdParams">命令参数集合</param>
        /// <returns>返回 DbDataReader 只进流</returns>
        public Task<DbDataReader> ExecuteReaderSpAsync(string spName, DbSqlParameter[] cmdParams = null)
        {
            return ExecuteReaderAsync(spName, cmdParams, CommandType.StoredProcedure);
        }

        #endregion

        #region Common

        /// <summary>
        /// 异步执行数据库语句或者调用存储过程，返回 DbDataReader 只进流
        /// </summary>
        /// <param name="objText">命令语句或者存储过程名称</param>
        /// <param name="cmdParams">命令参数集合</param>
        /// <param name="cmdType">如何解析命令字符串</param>
        /// <returns>返回 DbDataReader 只进流</returns>
        private Task<DbDataReader> ExecuteReaderAsync(string objText, DbSqlParameter[] cmdParams, CommandType cmdType)
        {
            using (var dbCommand = CreateDbCommand(objText, cmdParams, cmdType))
            {
                try
                {
                    return dbCommand.ExecuteReaderAsync(dbCommand.Transaction == null ? CommandBehavior.CloseConnection : CommandBehavior.Default);
                }
                catch (Exception ex) { throw ex; }
                finally
                {
                    dbCommand.DisposedConnection(false);
                }
            }
        }

        #endregion

        #endregion

        #region ExecuteNonQuery

        #region Text

        /// <summary>
        /// 异步执行命令语句
        /// </summary>
        /// <param name="sql">命令语句</param>
        /// <param name="cmdParams">命令参数集合</param>
        /// <returns>返回影响的记录数</returns>
        public Task<int> ExecuteNonQueryAsync(string sql, DbSqlParameter[] cmdParams = null)
        {
            return ExecuteNonQueryAsync(sql, cmdParams, CommandType.Text);
        }

        #endregion

        #region StoredProcedure

        /// <summary>
        /// 异步调用存储过程
        /// </summary>
        /// <param name="spName">存储过程名称</param>
        /// <param name="cmdParams">命令参数集合</param>
        /// <returns>返回影响的记录数</returns>
        public Task<int> ExecuteNonQuerySpAsync(string spName, DbSqlParameter[] cmdParams = null)
        {
            return ExecuteNonQueryAsync(spName, cmdParams, CommandType.StoredProcedure);
        }

        #endregion

        #region Common

        /// <summary>
        /// 异步执行数据库语句或者调用存储过程
        /// </summary>
        /// <param name="objText">命令语句或者存储过程名称</param>
        /// <param name="cmdParams">命令参数集合</param>
        /// <param name="cmdType">如何解析命令字符串</param>
        /// <returns>返回影响的记录数</returns>
        private Task<int> ExecuteNonQueryAsync(string objText, DbSqlParameter[] cmdParams, CommandType cmdType)
        {
            using (var dbCommand = CreateDbCommand(objText, cmdParams, cmdType))
            {
                try
                {
                    return dbCommand.ExecuteNonQueryAsync();
                }
                catch (Exception ex) { throw ex; }
                finally
                {
                    dbCommand.DisposedConnection();
                }
            }
        }

        #endregion

        #endregion

        #endregion

        #region 通用方法

        ///// <summary>
        /////  将字符串加上单引号
        ///// </summary>
        ///// <param name="value">要转换的值</param>
        ///// <returns>返回带单引号的字符串</returns>
        //public string ToSqlParamStr(dynamic value)
        //{
        //    if (value is int || value is decimal || value is long || value is float || value is double)
        //    {
        //        return value.ToString();
        //    }

        //    var lambda = new Func<string, string>((val) =>
        //    {
        //        return "'" + val.Replace("'", "''") + "'";
        //    });


        //    var thisStr = value?.ToString();

        //    if (RegexHelper.IsPickerDateTimeAsync(thisStr))
        //    {
        //        switch (ProviderName)
        //        {
        //            case "System.Data.SqlClient":
        //                return string.IsNullOrWhiteSpace(thisStr) ? "NULL" : lambda(value.ToString());
        //            case "System.Data.SQLite":
        //                return string.IsNullOrWhiteSpace(thisStr) ? "NULL" : lambda(value.ToString("s"));
        //            case "System.Data.OracleClient":
        //                return string.IsNullOrWhiteSpace(thisStr) ? "NULL" : lambda(string.Format("TO_DATE('{0}','YYYY-MM-DD HH24:MI:SS')", value.ToString("yyyy-MM-dd HH:mm:ss")));
        //            default:
        //                {
        //                    throw new Exception("未指定提供程序名称属性，关键字：ProviderName");
        //                }
        //        }
        //    }

        //    return lambda(thisStr);
        //}

        ///// <summary>
        ///// 将模糊查询的字段过滤
        ///// </summary>
        ///// <param name="value">要转换的字符串</param>
        ///// <returns>返回带百分号的字符串</returns>
        //public string ToSqlParamLikeStr(dynamic value)
        //{
        //    var lambda = new Func<string, string>((val) =>
        //    {
        //        return "'%" + val.Replace("'", "''") + "%'";
        //    });

        //    return lambda(value?.ToString());
        //}

        #endregion

        #region 接口实现

        /// <summary>
        /// 释放或重置非托管资源
        /// </summary>
        public void Dispose()
        {
            // TO DO Something , 秘密的
        }

        #endregion
    }

    /// <summary>
    /// 提供一个数据库访问上下文扩展
    /// </summary>
    public static partial class DbContextExt
    {
        /// <summary>
        /// 释放 DbCommand 关联的数据库连接对象
        /// </summary>
        /// <param name="dbCommand">关联的 DbCommand 连接对象</param>
        /// <param name="dbCommandDisposed">释放 DbCommand 的选择状态</param>
        public static void DisposedConnection(this DbCommand dbCommand, bool dbCommandDisposed = true)
        {
            if (dbCommand.Transaction == null && dbCommandDisposed)
            {
                dbCommand.Connection.Dispose();
            }
        }
    }
}