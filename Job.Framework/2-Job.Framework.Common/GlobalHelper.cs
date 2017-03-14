using System;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Job.Framework.Common
{
    /// <summary>
    /// 全局通用的帮助类
    /// </summary>
    public static class GlobalHelper
    {
        /// <summary>
        /// 获取一个截取字符（加自定义字符）
        /// </summary>
        /// <param name="input">内容</param>
        /// <param name="len">长度（中文，英文，数字等都当成一个字符）</param>
        /// <param name="type">省略样式</param>
        /// <returns>返回截取字符</returns>
        public static async Task<string> CutStringAsync(string input, int len, string type = "...")
        {
            if (string.IsNullOrWhiteSpace(input))
            {
                return await Task.FromResult
                (
                    result: string.Empty
                );
            }

            var q = input.ToCharArray();
            var temp = string.Empty;

            for (var i = 0; i < q.Length; i++)
            {
                if (i <= len - 1)
                {
                    temp += q[i];
                }

                if (i > len - 1)
                {
                    return await Task.FromResult
                    (
                        result: temp + type
                    );
                }
            }
            return await Task.FromResult
            (
                result: temp
            );
        }

        /// <summary>
        /// 清除 Html 标签
        /// </summary>
        /// <param name="htmlStr">含Html的内容</param>
        /// <param name="trimNbsp">是否去掉空格</param>
        /// <returns>返回清除结果</returns>
        public static async Task<string> ClearHtmlAsync(string htmlStr, bool trimNbsp = false)
        {
            if (string.IsNullOrWhiteSpace(htmlStr))
            {
                return await Task.FromResult
                (
                    result: string.Empty
                );
            }

            var str = Regex.Replace(htmlStr, "<[^>]*>", string.Empty);

            if (trimNbsp)
            {
                str = str.Replace("&nbsp;", string.Empty);
            }

            return await Task.FromResult
            (
                result: str
            );
        }

        /// <summary>
        /// 获取自定义域名的根域
        /// </summary>
        /// <param name="url">域名地址（www.gmbzero.cc，http://www.gmbzero.cc，https://gmbzero.cc）</param>
        /// <returns>返回根域</returns>
        public static async Task<string> GetRootDomainAsync(string url)
        {
            if (url.StartsWith("http://") == false && url.StartsWith("https://") == false)
            {
                url = "http://" + url;
            }

            var uri = new Uri(url);

            switch (uri.HostNameType)
            {
                case UriHostNameType.Dns:
                    {
                        if (uri.IsLoopback)
                        {
                            return uri.Host;
                        }

                        var host = uri.Host;
                        var hosts = host.Split('.');

                        return await Task.FromResult
                        (
                            result: hosts.Length == 2 ? host : string.Format("{0}.{1}", hosts[1], hosts[2])
                        );
                    }
                default:
                    {
                        return await Task.FromResult
                        (
                            result: uri.Host
                        );
                    }
            }
        }
    }
}
