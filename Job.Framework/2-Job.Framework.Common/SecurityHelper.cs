using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Job.Framework.Common
{
    /// <summary>
    /// 数据加密帮助类
    /// </summary>
    public static class SecurityHelper
    {
        private const string iv_128 = "!0ZEROBASELOVE0!";
        private const string key_128 = "!0LINGLINGLING0!";

        #region 单向加密

        /// <summary>
        /// 创建一个 MD5 字符
        /// </summary>
        /// <param name="input">需要加密的内容</param>
        /// <returns>返回加密结果</returns>
        public static async Task<string> MD5CreateAsync(string input)
        {
            return await MD5CreateAsync(await ConvertHelper.GetBytesAsync(Encoding.UTF8, input));
        }

        /// <summary>
        /// 创建一个安全的加盐 MD5 加密字符串
        /// </summary>
        /// <param name="input">需要加密的内容</param>
        /// <param name="salt">指定盐</param>
        /// <returns>返回加密结果</returns>
        public static async Task<string> MD5WithSaltAsync(string input, string salt = key_128)
        {
            return await MD5CreateAsync(await MD5CreateAsync(input) + salt);
        }

        /// <summary>
        /// 创建一个 MD5 字符
        /// </summary>
        /// <param name="input">需要加密的内容</param>
        /// <returns>返回加密结果</returns>
        public static async Task<string> MD5CreateAsync(byte[] input)
        {
            using (var md5 = MD5.Create())
            {
                var data = md5.ComputeHash(input);

                var sb = new StringBuilder();

                for (var i = 0; i < data.Length; i++)
                {
                    sb.AppendFormat("{0:x2}", data[i]);
                }

                return await Task.FromResult
                (
                    result: sb.ToString()
                );
            }
        }

        /// <summary>
        /// 创建一个 SHA1 字符
        /// </summary>
        /// <param name="input">需要加密的内容</param>
        /// <returns>返回加密结果</returns>
        public static async Task<string> SHA1CreateAsync(string input)
        {
            return await SHA1CreateAsync(await ConvertHelper.GetBytesAsync(Encoding.UTF8, input));
        }

        /// <summary>
        /// 创建一个安全的加盐 SHA1 加密字符串
        /// </summary>
        /// <param name="input">需要加密的内容</param>
        /// <param name="salt">指定盐</param>
        /// <returns>返回加密结果</returns>
        public static async Task<string> SHA1WithSaltAsync(string input, string salt = key_128)
        {
            return await SHA1CreateAsync(await SHA1CreateAsync(input) + salt);
        }

        /// <summary>
        /// 创建一个 SHA1 字符
        /// </summary>
        /// <param name="input">需要加密的内容</param>
        /// <returns>返回加密结果</returns>
        public static async Task<string> SHA1CreateAsync(byte[] input)
        {
            using (var sha1 = SHA1.Create())
            {
                var data = sha1.ComputeHash(input);

                var sb = new StringBuilder();

                for (var i = 0; i < data.Length; i++)
                {
                    sb.AppendFormat("{0:x2}", data[i]);
                }

                return await Task.FromResult
                (
                    result: sb.ToString()
                );
            }
        }

        #endregion
    }
}
