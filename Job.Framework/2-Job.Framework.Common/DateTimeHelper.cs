using System;
using System.Threading.Tasks;

namespace Job.Framework.Common
{
    /// <summary>
    /// 时间类型帮助类
    /// </summary>
    public static class DateTimeHelper
    {
        #region 时间戳操作

        /// <summary>
        /// 时间戳转换成时间格式
        /// </summary>
        /// <param name="timeSpan">时间戳</param>
        /// <returns>返回时间</returns>
        public static async Task<DateTime> ParseTimeSpanAsync(int timeSpan)
        {
            return await Task.FromResult
            (
                result: new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(timeSpan)
            );
        }

        /// <summary>
        /// 计算时间戳并返回结果
        /// </summary>
        /// <param name="dt">指定时间</param>
        /// <returns>返回时间戳</returns>
        public static async Task<int> GetTimeSpanAsync(DateTime dt)
        {
            return await Task.FromResult
            (
                result: (int)(dt - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds
            );
        }

        #endregion
    }
}
