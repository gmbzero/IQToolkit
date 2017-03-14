using System;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Job.Framework.Common
{
    /// <summary>
    /// 类型转换帮助类
    /// </summary>
    public static class ConvertHelper
    {
        #region 类型转换

        /// <summary>
        /// 尝试转换类型，如果转换失败则返回默认值
        /// </summary>
        /// <typeparam name="T">需要转换的类型</typeparam>
        /// <param name="value">需要转换的类型</param>
        /// <param name="defaultValue">转换失败返回的默认值</param>
        /// <returns>强制转换的结果</returns>
        public static async Task<T> TryParseAsync<T>(object value, object defaultValue = null)
        {
            try
            {
                return await ChangeTypeAsync<T>(value);
            }
            catch
            {
                return await ChangeTypeAsync<T>(defaultValue);
            }
        }

        /// <summary>
        /// 尝试转换类型，返回一个指定类型的对象，支持枚举的转换
        /// </summary>
        /// <typeparam name="T">需要转换的类型</typeparam>
        /// <param name="value">需要转换的值</param>
        /// <returns>强制转换的结果</returns>
        public static async Task<T> ChangeTypeAsync<T>(object value)
        {
            return await Task.FromResult
            (
                result: (T)(await ChangeTypeAsync(value, typeof(T)))
            );
        }

        /// <summary>
        /// 尝试转换类型，返回一个指定类型的对象，支持枚举的转换
        /// </summary>
        /// <param name="value">需要转换的值</param>
        /// <param name="conversionType">需要转换的类型</param>
        /// <returns>转换结果</returns>
        public static async Task<object> ChangeTypeAsync(object value, Type conversionType)
        {
            if (conversionType == null)
            {
                throw new ArgumentNullException("未定义需要转换的类型");
            }

            var typeInfo = conversionType.GetTypeInfo();

            if (value == null)
            {
                return typeInfo.IsValueType ? Activator.CreateInstance(conversionType) : null;
            }

            var nullableType = Nullable.GetUnderlyingType(conversionType);

            if (nullableType != null)
            {
                return Convert.ChangeType(value, nullableType);
            }

            if (typeInfo.BaseType == typeof(Enum))
            {
                return Enum.Parse(conversionType, value.ToString());
            }

            if (conversionType == typeof(Guid))
            {
                return new Guid(value.ToString());
            }

            if (conversionType == typeof(Version))
            {
                return new Version(value.ToString());
            }

            return await Task.FromResult
            (
                result: Convert.ChangeType(value, conversionType)
            );
        }

        #endregion

        #region 字节转换

        /// <summary>
        /// 以字节数组的形式返回指定的字节视图
        /// </summary>
        /// <param name="value">要转换的字节视图</param>
        /// <returns>返回字节数组</returns>
        public static async Task<byte[]> GetBytesAsync(Stream value)
        {
            using (var mstream = new MemoryStream())
            {
                var bytes = new byte[1024];

                if (value.CanRead)
                {
                    while (true)
                    {
                        var length = await value.ReadAsync(bytes, 0, bytes.Length);

                        if (length <= 0)
                        {
                            break;
                        }

                        await mstream.WriteAsync(bytes, 0, length);
                    }
                }

                return await Task.FromResult
                (
                    result: mstream.ToArray()
                );
            }
        }

        /// <summary>
        /// 异步以字节数组的形式返回指定的字符串
        /// </summary>
        /// <param name="encoding">字符编码</param>
        /// <param name="value">要转换的字符串</param>
        /// <returns>返回字节数组</returns>
        public static async Task<byte[]> GetBytesAsync(Encoding encoding, string value)
        {
            return await Task.FromResult
            (
                result: encoding.GetBytes(value)
            );
        }

        /// <summary>
        /// 返回由字节数组中指定位置的字符串
        /// </summary>
        /// <param name="encoding">字符编码</param>
        /// <param name="value">要转换的字节视图</param>
        /// <returns>返回字符串</returns>
        public static async Task<string> GetStringAsync(Encoding encoding, Stream value)
        {
            return await GetStringAsync(encoding, await GetBytesAsync(value));
        }

        /// <summary>
        /// 返回由字节数组中指定位置的字符串
        /// </summary>
        /// <param name="encoding">字符编码</param>
        /// <param name="value">字节数组</param>
        /// <returns>返回字符串</returns>
        public static async Task<string> GetStringAsync(Encoding encoding, byte[] value)
        {
            return await Task.FromResult
            (
                result: encoding.GetString(value)
            );
        }

        /// <summary>
        /// 以字节数组的形式返回指定的 32 位有符号整数值
        /// </summary>
        /// <param name="value">要转换的数字</param>
        /// <returns>长度为 4 的字节数组</returns>
        public static async Task<byte[]> GetBytesAsync(int value)
        {
            return await Task.FromResult
            (
                result: BitConverter.GetBytes(value)
            );
        }

        /// <summary>
        /// 返回由字节数组中指定位置的四个字节转换来的 32 位有符号整数
        /// </summary>
        /// <param name="value">字节数组</param>
        /// <param name="startIndex">value 内的起始位置</param>
        /// <returns>由四个字节构成、从 startIndex 开始的 32 位有符号整数</returns>
        public static async Task<int> ToInt32Async(byte[] value, int startIndex = 0)
        {
            return await Task.FromResult
            (
               result: BitConverter.ToInt32(value, startIndex)
            );
        }

        /// <summary>
        /// 以字节数组的形式返回指定的双精度浮点值
        /// </summary>
        /// <param name="value">要转换的数字</param>
        /// <returns>长度为 8 的字节数组</returns>
        public static async Task<byte[]> GetBytesAsync(double value)
        {
            return await Task.FromResult
            (
               result: BitConverter.GetBytes(value)
            );
        }

        /// <summary>
        /// 返回由字节数组中指定位置的八个字节转换来的双精度浮点数
        /// </summary>
        /// <param name="value">字节数组</param>
        /// <param name="startIndex">value 内的起始位置</param>
        /// <returns>由八个字节构成、从 startIndex 开始的双精度浮点数</returns>
        public static async Task<double> ToDoubleAsync(byte[] value, int startIndex = 0)
        {
            return await Task.FromResult
            (
               result: BitConverter.ToDouble(value, startIndex)
            );
        }

        /// <summary>
        /// 以字节数组的形式返回指定的布尔值
        /// </summary>
        /// <param name="value">一个布尔值</param>
        /// <returns>长度为 1 的字节数组</returns>
        public static async Task<byte[]> GetBytesAsync(bool value)
        {
            return await Task.FromResult
            (
               result: BitConverter.GetBytes(value)
            );
        }

        /// <summary>
        /// 返回由字节数组中指定位置的一个字节转换来的布尔值
        /// </summary>
        /// <param name="value">字节数组</param>
        /// <param name="startIndex">value 内的起始位置</param>
        /// <returns>如果 value 中的 startIndex 处的字节非零，则为 true；否则为 false</returns>
        public static async Task<bool> ToBooleanAsync(byte[] value, int startIndex = 0)
        {
            return await Task.FromResult
            (
               result: BitConverter.ToBoolean(value, startIndex)
            );
        }

        /// <summary>
        /// 以字节数组的形式返回指定的 Unicode 字符值
        /// </summary>
        /// <param name="value">要转换的字符</param>
        /// <returns>长度为 2 的字节数组</returns>
        public static async Task<byte[]> GetBytesAsync(char value)
        {
            return await Task.FromResult
            (
               result: BitConverter.GetBytes(value)
            );
        }

        /// <summary>
        /// 返回由字节数组中指定位置的两个字节转换来的 Unicode 字符
        /// </summary>
        /// <param name="value">一个数组</param>
        /// <param name="startIndex">value 内的起始位置</param>
        /// <returns>由两个字节构成、从 startIndex 开始的字符</returns>
        public static async Task<char> ToCharAsync(byte[] value, int startIndex = 0)
        {
            return await Task.FromResult
            (
               result: BitConverter.ToChar(value, startIndex)
            );
        }

        /// <summary>
        /// 以字节数组的形式返回指定的单精度浮点值
        /// </summary>
        /// <param name="value">要转换的数字</param>
        /// <returns>长度为 4 的字节数组</returns>
        public static async Task<byte[]> GetBytesAsync(float value)
        {
            return await Task.FromResult
            (
               result: BitConverter.GetBytes(value)
            );
        }

        /// <summary>
        /// 返回由字节数组中指定位置的四个字节转换来的单精度浮点数
        /// </summary>
        /// <param name="value">字节数组</param>
        /// <param name="startIndex">value 内的起始位置</param>
        /// <returns>由四个字节构成、从 startIndex 开始的单精度浮点数</returns>
        public static async Task<float> ToSingleAsync(byte[] value, int startIndex = 0)
        {
            return await Task.FromResult
            (
               result: BitConverter.ToSingle(value, startIndex)
            );
        }

        /// <summary>
        /// 以字节数组的形式返回指定的 64 位有符号整数值
        /// </summary>
        /// <param name="value">要转换的数字</param>
        /// <returns>长度为 8 的字节数组</returns>
        public static async Task<byte[]> GetBytesAsync(long value)
        {
            return await Task.FromResult
            (
               result: BitConverter.GetBytes(value)
            );
        }

        /// <summary>
        /// 返回由字节数组中指定位置的八个字节转换来的 64 位有符号整数
        /// </summary>
        /// <param name="value">字节数组</param>
        /// <param name="startIndex">value 内的起始位置</param>
        /// <returns>由八个字节构成、从 startIndex 开始的 64 位有符号整数</returns>
        public static async Task<long> ToInt64Async(byte[] value, int startIndex = 0)
        {
            return await Task.FromResult
            (
               result: BitConverter.ToInt64(value, startIndex)
            );
        }
        
        #endregion
    }
}
