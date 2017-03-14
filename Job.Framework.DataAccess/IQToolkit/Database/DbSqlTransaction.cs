using System;
using System.Linq;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;

namespace Job.Framework.DataAccess
{
    /// <summary>
    /// 表示事务机制常用基础类
    /// </summary>
    public sealed class DbSqlTransaction : IDisposable
    {
        #region 内部使用

        /// <summary>
        /// 内部使用的事务状态类
        /// </summary>
        internal class TransationState
        {
            /// <summary>
            /// 数据库事务连接池
            /// </summary>
            public ConcurrentDictionary<string, DbTransaction> TransactionPool { get; set; }

            /// <summary>
            /// 提交事务标志
            /// </summary>
            public bool IsCommit { get; set; }
        }

        #endregion

        #region 相关属性

        /// <summary>
        /// 存储在线程里的静态变量
        /// </summary>
        private static AsyncLocal<TransationState> StateCurrent = new AsyncLocal<TransationState>();

        /// <summary>
        /// 获取事务状态是否已经启用
        /// </summary>
        internal static bool IsBeginTransaction
        {
            get
            {
                return StateCurrent.Value != null;
            }
        }

        /// <summary>
        /// 获取当前开启是否为顶级事务
        /// </summary>
        internal bool IsParent { get; private set; }

        #endregion

        #region 构造函数

        /// <summary>
        /// 初始化对象实例
        /// </summary>
        public DbSqlTransaction()
        {
            if (IsParent == false && StateCurrent.Value == null)
            {
                if ((IsParent = true) == true)
                {
                    StateCurrent.Value = new TransationState
                    {
                        TransactionPool = new ConcurrentDictionary<string, DbTransaction>(),
                        IsCommit = false
                    };
                }
            }
        }

        #endregion

        #region 事务方法

        /// <summary>
        /// 设置指定的事务
        /// </summary>
        /// <param name="connectionKey">数据库连接键值</param>
        /// <param name="transaction">事务的基类对象</param>
        public static void Set(string connectionKey, DbTransaction transaction)
        {
            if (StateCurrent.Value == null)
            {
                throw new ArgumentNullException("事务操作失败，可能是还未真正地开启事务命令操作");
            }

            StateCurrent.Value.TransactionPool.AddOrUpdate(connectionKey, transaction, (key, value) =>
            {
                return transaction;
            });
        }

        /// <summary>
        /// 获取指定的事务
        /// </summary>
        /// <param name="connectionKey">数据库连接键值</param>
        /// <returns>返回事务的基类对象</returns>
        public static DbTransaction Get(string connectionKey)
        {
            if (StateCurrent.Value == null || StateCurrent.Value.TransactionPool.ContainsKey(connectionKey) == false)
            {
                return null;
            }

            return StateCurrent.Value.TransactionPool[connectionKey];
        }

        /// <summary>
        /// 对当前挂起事务进行回滚
        /// </summary>
        public void Rollback()
        {
            StateCurrent.Value.IsCommit = false;
        }

        /// <summary>
        /// 对当前挂起事务进行提交
        /// </summary>
        public void Commit()
        {
            StateCurrent.Value.IsCommit = true;
        }

        /// <summary>
        /// 释放资源，对事务进行判断
        /// </summary>
        public void Dispose()
        {
            if (IsParent && StateCurrent.Value.TransactionPool.Count > 0)
            {
                foreach (var item in StateCurrent.Value.TransactionPool.Values)
                {
                    using (var connect = item.Connection)
                    {
                        if (StateCurrent.Value.IsCommit == false)
                        {
                            item.Rollback();
                        }
                        else
                        {
                            item.Commit();
                        }
                    }
                }

                StateCurrent.Value = null; IsParent = false;
            }
        }

        #endregion
    }
}
