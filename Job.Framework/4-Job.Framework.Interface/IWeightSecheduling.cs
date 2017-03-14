namespace Job.Framework.Interface
{
    /// <summary>
    /// 定义一个权重规则接口
    /// </summary>
    public interface IWeightSecheduling
    {
        /// <summary>
        /// 获取权重值
        /// </summary>
        int Weight { get; }
    }
}
