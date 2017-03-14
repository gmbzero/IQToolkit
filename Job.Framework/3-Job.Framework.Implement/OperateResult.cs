using Job.Framework.Common;
using Job.Framework.Interface;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Job.Framework.Implement
{
    /// <summary>
    /// 表示一个操作结果的封装
    /// </summary>
    public class OperateResult : IOperateResult
    {
        #region 相关属性

        /// <summary>
        /// 获取操作结果信息
        /// </summary>
        public string Message { get; }

        /// <summary>
        /// 获取操作结果状态
        /// </summary>
        public OperateStatus Status { get; }

        #endregion

        #region 构造函数

        /// <summary>
        /// 初始化实例对象
        /// </summary>
        /// <param name="status">操作结果状态</param>
        /// <param name="message">操作结果信息</param>
        protected OperateResult(OperateStatus status, string message)
        {
            this.Status = status;
            this.Message = message;
        }

        #endregion

        #region 相关方法

        /// <summary>
        /// 创建一个操作结果
        /// </summary>
        /// <param name="status">操作结果状态</param>
        /// <param name="message">操作结果信息</param>
        /// <returns>一个操作结果封装对象</returns>
        public static async Task<IOperateResult> CreateAsync(OperateStatus status, string message)
        {
            return await Task.FromResult
            (
                result: new OperateResult
                (
                    status: status,
                    message: message
                )
            );
        }

        #endregion
    }
    
    /// <summary>
    /// 表示一个实现泛型操作结果的封装
    /// </summary>
    /// <typeparam name="T">操作返回值需要转换的类型</typeparam>
    public sealed class OperateResult<T> : OperateResult, IOperateResult<T>
    {
        #region 相关属性

        /// <summary>
        /// 获取一个操作结果返回值
        /// </summary>
        public T Value { get; private set; }

        #endregion

        #region 构造函数

        /// <summary>
        /// 初始化实例对象
        /// </summary>
        /// <param name="status">操作结果状态</param>
        /// <param name="message">操作结果信息</param>
        /// <param name="value">操作结果返回值</param>
        public OperateResult(OperateStatus status, string message, T value) : base(status, message)
        {
            this.Value = value;
        }

        #endregion

        #region 相关方法

        /// <summary>
        /// 创建一个操作结果
        /// </summary>
        /// <param name="status">操作结果状态</param>
        /// <param name="message">操作结果信息</param>
        /// <param name="value">操作结果返回值</param>
        /// <returns>一个操作结果封装对象</returns>
        public static async Task<IOperateResult> CreateAsync(OperateStatus status, string message, T value)
        {
            return await Task.FromResult
            (
                result: new OperateResult<T>
                (
                    status: status,
                    message: message,
                    value: value
                )
            );
        }

        #endregion
    }
}
