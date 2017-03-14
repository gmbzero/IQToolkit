using System;
using System.Runtime.InteropServices;
using System.Text;

namespace Job.Framework.Common
{
    /// <summary>
    /// 非托管动态链接库注入帮助类
    /// </summary>
    public static class DllImportHelper
    {
        /// <summary>
        /// 指示由非托管动态链接库 (DLL) 作为静态入口点公开
        /// </summary>
        /// <param name="hwnd">指定父窗口句柄</param>
        /// <param name="lpszOp">指定动作</param>
        /// <param name="lpszFile">指定要打开的文件或程序</param>
        /// <param name="lpszParams">给要打开的程序指定参数</param>
        /// <param name="lpszDir">缺省目录</param>
        /// <param name="FsShowCmd">打开选项</param>
        /// <returns>返回值大于32表示执行成功，返回值小于32表示执行错误</returns>
        [DllImport("shell32.dll")]
        public static extern int ShellExecute(IntPtr hwnd, StringBuilder lpszOp, StringBuilder lpszFile, StringBuilder lpszParams, StringBuilder lpszDir, int FsShowCmd);
    }
}