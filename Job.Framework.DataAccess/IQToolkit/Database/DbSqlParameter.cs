using System.Data;

namespace Job.Framework.DataAccess
{
    /// <summary>
    /// 表示数据库参数传递容器
    /// </summary>
    public sealed class DbSqlParameter
    {
        /// <summary>
        /// 获取或设置参数的名称
        /// </summary>
        public string ParameterName { get; set; }

        /// <summary>
        /// 获取或设置该参数的值
        /// </summary>
        public object Value { get; set; }

        /// <summary>
        /// 获取或设置一个值，该值指示参数是只可输入、只可输出、双向还是存储过程返回值参数
        /// </summary>
        public ParameterDirection Direction { get; set; }
    }
}
