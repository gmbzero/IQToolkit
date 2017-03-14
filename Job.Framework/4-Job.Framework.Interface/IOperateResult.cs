namespace Job.Framework.Interface
{
    /// <summary>
    /// 定义操作结果状态
    /// </summary>
    public enum OperateStatus
    {
        /// <summary>
        /// 返回失败
        /// </summary>
        Failed = 0,

        /// <summary>
        /// 返回成功
        /// </summary>
        Success = 1
    }

    /// <summary>
    /// 定义一个操作结果规则接口
    /// </summary>
    public interface IOperateResult
    {
        /// <summary>
        /// 获取操作结果信息
        /// </summary>
        string Message { get; }

        /// <summary>
        /// 获取操作结果状态
        /// </summary>
        OperateStatus Status { get; }
    }

    /// <summary>
    /// 定义一个泛型的操作结果规则接口
    /// </summary>
    /// <typeparam name="T">操作返回值需要转换的类型</typeparam>
    public interface IOperateResult<T> : IOperateResult
    {
        /// <summary>
        /// 获取一个操作结果返回值
        /// </summary>
        T Value { get; }
    }
}
