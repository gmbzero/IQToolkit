using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Job.Framework.DataAccess
{
    /// <summary>
    /// 提供一个数据库访问上下文配置
    /// </summary>
    public class DbContextOptins
    {
        /// <summary>
        /// 获取或设置一个数据库连接对象
        /// </summary>
        internal DbConnection DbConnection { get; set; }

        /// <summary>
        /// 获取或设置参数前缀符号，默认为：@
        /// </summary>
        internal string ParameterPrefix { get; set; } = "@";

        /// <summary>
        /// 获取或设置等待命令执行的时间（以秒为单位），默认为：30 秒
        /// </summary>
        internal int CommandTimeout { get; set; } = 30;
    }

    /// <summary>
    /// 提供一个数据库访问上下文配置扩展方法
    /// </summary>
    public static class DbContextOptionsExt
    {
        /// <summary>
        /// 使用一个数据库连接访问对象，支持多数据库的调用
        /// </summary>
        /// <param name="options">数据库访问上下文配置扩</param>
        /// <param name="dbConnection">数据库连接访问对象</param>
        public static void UseDbConnection(this DbContextOptins options, DbConnection dbConnection)
        {
            options.DbConnection = dbConnection;
        }

        /// <summary>
        /// 对数据库命令操作时使用的参数前缀字符
        /// </summary>
        /// <param name="options">数据库访问上下文配置扩</param>
        /// <param name="parameterPrefix">参数前缀字符</param>
        public static void UseParameterPrefix(this DbContextOptins options, string parameterPrefix)
        {
            options.ParameterPrefix = parameterPrefix;
        }

        /// <summary>
        /// 对数据库命令操作时设置超时时间
        /// </summary>
        /// <param name="options">数据库访问上下文配置扩</param>
        /// <param name="commandTimeout">数据库命令操作时超时时间</param>
        public static void UseCommandTimeout(this DbContextOptins options, int commandTimeout)
        {
            options.CommandTimeout = commandTimeout;
        }
    }
}
