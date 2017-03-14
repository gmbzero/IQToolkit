using System;
using System.Threading;
using System.Threading.Tasks;

namespace Job.Framework.Common
{
    /// <summary>
    /// 可排序的 COMB 类型 GUID
    /// </summary>
    public static class GuidHelper
    {
        private static readonly DateTime combBaseDate = new DateTime(1900, 1, 1);
        private static int lastDays;
        private static int lastTenthMilliseconds;

        /// <summary>
        /// 初始化 System.Guid 结构的新实例
        /// </summary>
        /// <returns>一个新的 GUID 对象</returns>
        public static async Task<Guid> NewGuidAsync()
        {
            var now = DateTime.Now;
            var days = new TimeSpan(now.Ticks - combBaseDate.Ticks).Days;
            var guidArray = Guid.NewGuid().ToByteArray();
            var tenthMilliseconds = (Int32)(now.TimeOfDay.TotalMilliseconds * 10D);

            if (days > lastDays)
            {
                Interlocked.CompareExchange(ref lastDays, days, lastDays);
                Interlocked.CompareExchange(ref lastTenthMilliseconds, tenthMilliseconds, lastTenthMilliseconds);
            }
            else
            {
                if (tenthMilliseconds > lastTenthMilliseconds)
                {
                    Interlocked.CompareExchange(ref lastTenthMilliseconds, tenthMilliseconds, lastTenthMilliseconds);
                }
                else
                {
                    if (lastTenthMilliseconds < Int32.MaxValue)
                    {
                        Interlocked.Increment(ref lastTenthMilliseconds);
                    }

                    tenthMilliseconds = lastTenthMilliseconds;
                }
            }

            var daysArray = await ConvertHelper.GetBytesAsync((long)days);
            var msecsArray = await ConvertHelper.GetBytesAsync((long)tenthMilliseconds);

            Array.Reverse(daysArray);
            Array.Reverse(msecsArray);

            Array.Copy(daysArray, daysArray.Length - 2, guidArray, guidArray.Length - 6, 2);
            Array.Copy(msecsArray, 0, guidArray, guidArray.Length - 4, 4);

            return await Task.FromResult
            (
                result: new Guid(guidArray)
            );
        }
    }
}