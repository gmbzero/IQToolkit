using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Job.Framework.Interface
{
    /// <summary>
    /// 定义一个身份认证规则接口
    /// </summary>
    /// <typeparam name="T">身份信息类型</typeparam>
    public interface IAuthentication<T> where T : class
    {
        /// <summary>
        /// 判断是否通过了身份认证
        /// </summary>
        bool IsAuthenticated { get; }

        /// <summary>
        /// 获取身份信息主体
        /// </summary>
        T Identity { get; }

        /// <summary>
        /// 登录操作
        /// </summary>
        /// <param name="id">帐号唯一标识</param>
        /// <param name="userAccount">登录帐号</param>
        /// <param name="isPersistent">是否长期保存</param>
        /// <returns>操作结果</returns>
        IOperateResult SignIn(object id, string userAccount, bool isPersistent = false);

        /// <summary>
        /// 注销操作
        /// </summary>
        /// <returns>操作结果</returns>
        IOperateResult SignOut();
    }
}
