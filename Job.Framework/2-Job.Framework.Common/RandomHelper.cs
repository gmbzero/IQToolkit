using System;
using System.Security.Cryptography;
using System.Threading.Tasks;

namespace Job.Framework.Common
{
    /// <summary>
    /// 随机数据的帮助类
    /// </summary>
    public static class RandomHelper
    {
        /// <summary>
        /// 获取一个随机种子
        /// </summary>
        /// <returns>返回随机种子</returns>
        public static async Task<int> RandomSeedAsync()
        {
            using (var rng = RandomNumberGenerator.Create())
            {
                var bytes = new byte[4];

                rng.GetBytes(bytes);

                return await ConvertHelper.ToInt32Async(bytes, 0);
            }
        }

        /// <summary>
        /// 获取一个指定长度的随机 32位 有符号整数
        /// </summary>
        /// <param name="length">指定长度</param>
        /// <returns>返回随机整数</returns>
        public static async Task<int> NextInt32Async(int length)
        {
            return await ConvertHelper.ChangeTypeAsync<int>
            (
                value: await NextIntAsync(length)
            );
        }

        /// <summary>
        /// 获取一个指定长度的随机 64位 有符号整数
        /// </summary>
        /// <param name="length">指定长度</param>
        /// <returns>返回随机整数</returns>
        public static async Task<long> NextInt64Async(int length)
        {
            return await ConvertHelper.ChangeTypeAsync<long>
            (
                value: await NextIntAsync(length)
            );
        }

        /// <summary>
        /// 获取一个指定长度的随机有符号整数
        /// </summary>
        /// <param name="length">指定长度</param>
        /// <returns>返回随机整数</returns>
        public static async Task<string> NextIntAsync(int length)
        {
            return await NextStringAsync(length, new string[] { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9" });
        }

        /// <summary>
        /// 获取一个指定长度的随机字符
        /// </summary>
        /// <param name="length">指定长度</param>
        /// <param name="array">指定随机数据源</param>
        /// <returns>返回随机字符</returns>
        public static async Task<string> NextStringAsync(int length, string[] array)
        {
            var str = string.Empty;

            var rand = new Random(await RandomSeedAsync());

            for (var i = 0; i < length; i++)
            {
                str += array[rand.Next(0, array.Length - 1)];
            }

            return await Task.FromResult
            (
                result: str
            );
        }

        /// <summary>
        /// 获取一个非负随机整数
        /// </summary>
        /// <returns>返回一个非负随机整数</returns>
        public static async Task<int> NextAsync()
        {
            return await Task.FromResult
            (
                result: new Random(await RandomSeedAsync()).Next()
            );
        }

        /// <summary>
        /// 获取一个小于所指定最大值的非负随机整数
        /// </summary>
        /// <param name="maxValue">要生成的随机数的上限（随机数不能取该上限值），maxValue 必须大于或等于 0</param>
        /// <returns>返回大于等于零且小于 maxValue 的 32 位带符号整数</returns>
        public static async Task<int> NextAsync(int maxValue)
        {
            return await Task.FromResult
            (
                result: new Random(await RandomSeedAsync()).Next(maxValue)
            );
        }

        /// <summary>
        /// 获取一个指定范围内的任意整数
        /// </summary>
        /// <param name="minValue">最小值</param>
        /// <param name="maxValue">最大值</param>
        /// <returns>返回一个指定范围内的任意整数</returns>
        public static async Task<int> NextAsync(int minValue, int maxValue)
        {
            return await Task.FromResult
            (
                result: new Random(await RandomSeedAsync()).Next(minValue, maxValue)
            );
        }

        /// <summary>
        /// 获取一个大于或等于 0.0 且小于 1.0 的随机浮点数
        /// </summary>
        /// <returns>返回一个大于或等于 0.0 且小于 1.0 的随机浮点数</returns>
        public static async Task<double> NextDoubleAsync()
        {
            return await Task.FromResult
            (
                result: new Random(await RandomSeedAsync()).NextDouble()
            );
        }

        /// <summary>
        /// 获取用随机数填充指定字节数组的元素
        /// </summary>
        /// <param name="buffer">字节数组</param>
        public static async Task NextBytesAsync(byte[] buffer)
        {
            await Task.Run(async () =>
            {
                new Random(await RandomSeedAsync()).NextBytes(buffer);
            });
        }
    }
}
