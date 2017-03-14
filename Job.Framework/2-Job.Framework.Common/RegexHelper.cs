using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Job.Framework.Common
{
    /// <summary>
    /// 正则表达式帮助类
    /// </summary>
    public static class RegexHelper
    {
        #region Guid

        /// <summary>
        /// 定义 Guid 正则表达式
        /// </summary>
        public const string GuidPattern = @"^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$";

        /// <summary>
        /// 判断是否是 Guid 格式
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsGuidAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, GuidPattern, RegexOptions.IgnoreCase)
            );
        }

        #endregion

        #region 汉字

        /// <summary>
        /// 定义汉字正则表达式
        /// </summary>
        public const string ChineseCharacterPattern = @"^[\u4e00-\u9fa5]$";

        /// <summary>
        /// 判断是否为汉字
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsChineseCharacterAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, ChineseCharacterPattern)
            );
        }

        #endregion

        #region 信用卡

        /// <summary>
        /// 定义信用卡正则表达式
        /// </summary>
        public const string CreditCardPattern = @"^(4\d{12}(?:\d{3})?)$";

        /// <summary>
        /// 判断是否为信用卡
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsCreditCardAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, CreditCardPattern)
            );
        }

        #endregion

        #region 正整数

        /// <summary>
        /// 定义正整数正则表达式
        /// </summary>
        public const string DigitsPattern = @"^\d+$";

        /// <summary>
        /// 判断是否为正整数
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsDigitsAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, DigitsPattern)
            );
        }

        #endregion

        #region 邮箱地址

        /// <summary>
        /// 定义邮箱地址正则表达式
        /// </summary>
        public const string EmailAddressPattern = @"^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$";

        /// <summary>
        /// 判断是否为邮箱地址
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsEmailAddressAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, EmailAddressPattern)
            );
        }

        #endregion

        #region 传真号码

        /// <summary>
        /// 定义传真号码正则表达式
        /// </summary>
        public const string FaxNumberPattern = @"^[0-9-]+$";

        /// <summary>
        /// 判断是否为传真号码
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsFaxNumberAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, FaxNumberPattern)
            );
        }

        #endregion

        #region 身份证

        /// <summary>
        /// 定义身份证正则表达式
        /// </summary>
        public const string IdentityCardPattern = @"^(^\d{15}$)|(\d{17}(?:\d|x|X)$)$";

        /// <summary>
        /// 判断是否为身份证
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsIdentityCardAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, IdentityCardPattern)
            );
        }

        #endregion

        #region IP地址

        /// <summary>
        /// 定义IP地址正则表达式
        /// </summary>
        public const string IPAddressPattern = @"^(\d+)\.(\d+)\.(\d+)\.(\d+)$";

        /// <summary>
        /// 判断是否为IP地址
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsIPAddressAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, IPAddressPattern)
            );
        }

        #endregion

        #region 数字

        /// <summary>
        /// 定义数字正则表达式
        /// </summary>
        public const string NumberPattern = @"^[(-?\d+\.\d+)|(-?\d+)|(-?\.\d+)]+$";

        /// <summary>
        /// 判断是否为数字
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsNumberAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, NumberPattern)
            );
        }

        #endregion

        #region 手机号码

        /// <summary>
        /// 定义手机号码正则表达式
        /// </summary>
        public const string PhoneNumberPattern = @"^(1[3-9])\d{9}$";

        /// <summary>
        /// 判断是否为手机号码
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsPhoneNumberAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, PhoneNumberPattern)
            );
        }

        #endregion

        #region 邮政编号

        /// <summary>
        /// 定义邮政编号正则表达式
        /// </summary>
        public const string PostCodePattern = @"^[0-9]{6}$";

        /// <summary>
        /// 判断是否为邮政编号
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsPostCodeAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, PostCodePattern)
            );
        }

        #endregion

        #region QQ

        /// <summary>
        /// 定义 QQ 正则表达式
        /// </summary>
        public const string QQPattern = @"^[1-9][0-9]{4,}$";

        /// <summary>
        /// 判断是否是QQ
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsQQAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, QQPattern)
            );
        }

        #endregion

        #region 固话

        /// <summary>
        /// 定义固话正则表达式
        /// </summary>
        public const string TelephonePattern = @"^(\(\d{3,4}\)|\d{3,4}-)?\d{7,8}(-\d{1,4})?$";

        /// <summary>
        /// 判断是否为国内固话（0511 - 4405222 或 021 - 87888822）
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsTelephoneAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, TelephonePattern)
            );
        }

        #endregion

        #region Url

        /// <summary>
        /// 定义 Url 正则表达式
        /// </summary>
        public const string UrlAddressPattern = @"^(http|https|ftp)://[a-zA-Z0-9-.]+.[a-zA-Z]{2,3}(:[a-zA-Z0-9]*)?/?([a-zA-Z0-9-._?,'/\+&%$#=~])*[^.,)(s]$";

        /// <summary>
        /// 判断是否为有效的 Url 地址
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsUrlAddressAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, UrlAddressPattern)
            );
        }

        #endregion

        #region 双字节

        /// <summary>
        /// 定义双字节正则表达式
        /// </summary>
        public const string DoubleCharacterPattern = @"^[^\x00-\xff]$";

        /// <summary>
        /// 判断是否为双字节字符（包括汉字在内）
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsDoubleCharacterAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, DoubleCharacterPattern)
            );
        }

        #endregion

        #region 日期

        /// <summary>
        /// 定义日期正则表达式
        /// </summary>
        public const string PickerDatePattern = @"^(?:(?!0000)[0-9]{4}([-/.]?)(?:(?:0?[1-9]|1[0-2])\1(?:0?[1-9]|1[0-9]|2[0-8])|(?:0?[13-9]|1[0-2])\1(?:29|30)|(?:0?[13578]|1[02])\1(?:31))|(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)([-/.]?)0?2\2(?:29))$";

        /// <summary>
        /// 判断是否为日期（20151111，2015-11-11，2015/11/11，2015.11.11）
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsPickerDateAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, PickerDatePattern)
            );
        }

        #endregion

        #region 月份

        /// <summary>
        /// 定义月份正则表达式
        /// </summary>
        public const string PickerMonthPattern = @"^(?:(?!0000)[0-9]{4}([-/.]?)(?:(?:0?[1-9]|1[0-2])|(?:0?[13-9]|1[0-2])|(?:0?[13578]|1[02])))$";

        /// <summary>
        /// 判断是否为月份（201511，2015-11，2015/11，2015.11）
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsPickerMonthAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, PickerMonthPattern)
            );
        }

        #endregion

        #region 年份

        /// <summary>
        /// 定义年份正则表达式
        /// </summary>
        public const string PickerYearPattern = @"^(?:(?!0000)[0-9]{4})$";

        /// <summary>
        /// 判断是否为年份（2015）
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsPickerYearAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, PickerYearPattern)
            );
        }

        #endregion

        #region 时间

        /// <summary>
        /// 定义时间正则表达式
        /// </summary>
        public const string PickerTimePattern = @"^([01][0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9]){0,1}$";

        /// <summary>
        /// 判断是否为时间（11:11，11:11:11）
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsPickerTimeAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, PickerTimePattern)
            );
        }

        #endregion

        #region 完整日期

        /// <summary>
        /// 定义完整日期正则表达式
        /// </summary>
        public const string DateTimePattern = @"^(?:(?!0000)[0-9]{4}[-/.](?:(?:0[1-9]|1[0-2])[-/.](?:0[1-9]|1[0-9]|2[0-8])|(?:0[13-9]|1[0-2])[-/.](?:29|30)|(?:0[13578]|1[02])[-/.]31)|(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)[-/.]02[-/.]29)\s+([01][0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9]){0,1}$";

        /// <summary>
        /// 判断是否为完整日期（2015-11-11，2015/11/11，2015.11.11 11:11:11）
        /// </summary>
        /// <param name="input">内容</param>
        /// <returns>返回真假结果</returns>
        public static async Task<bool> IsPickerDateTimeAsync(string input)
        {
            return await Task.FromResult
            (
                result: Regex.IsMatch(input ?? string.Empty, DateTimePattern)
            );
        }

        #endregion
    }
}
