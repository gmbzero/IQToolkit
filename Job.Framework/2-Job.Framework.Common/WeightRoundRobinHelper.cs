using Job.Framework.Interface;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Job.Framework.Common
{
    /// <summary>
    /// 权重轮询调度算法帮助类
    /// </summary>
    public static class WeightRoundRobinHelper
    {
        /// <summary>
        /// 存储轮询位置以及权重信息
        /// </summary>
        private class CurrentInfo
        {
            public int Index { get; set; }
            public int Weight { get; set; }
        }

        /// <summary>
        /// 存储轮询位置以及权重信息字典
        /// </summary>
        private static ConcurrentDictionary<string, CurrentInfo> Current = new ConcurrentDictionary<string, CurrentInfo>();

        /// <summary>
        /// 通过权重的不断递减寻找适合的数据返回，直到轮询结束 
        /// </summary>
        /// <typeparam name="T">继承 IRoundRobinWeight 的权重对象</typeparam>
        /// <param name="key">权重轮询的组名称</param>
        /// <param name="list">权重轮询的抽取列表</param>
        /// <returns>返回权重轮询的结果</returns>
        public static async Task<T> GetAsync<T>(string key, List<T> list) where T : IWeightSecheduling
        {
            var maxWeight = list.Max(e => e.Weight);            //最大权重 
            var gcdWeight = await GetGcdWeightAsync(list);      //最大公约数
            var listCount = list.Count;                         //列表数量

            var current = Current.GetOrAdd(key, new CurrentInfo
            {
                Index = -1,                      //上一次的选择
                Weight = 0                       //当前调度的权重
            });

            while (true)
            {
                current.Index = (current.Index + 1) % listCount;

                if (current.Index == 0)
                {
                    current.Weight = current.Weight - gcdWeight;

                    if (current.Weight <= 0)
                    {
                        current.Weight = maxWeight;

                        if (current.Weight == 0)
                        {
                            return await Task.FromResult
                            (
                                result: default(T)
                            );
                        }
                    }
                }

                if (list[current.Index].Weight >= current.Weight)
                {
                    return await Task.FromResult
                    (
                        result: list[current.Index]
                    );
                }
            }
        }

        /// <summary>
        /// 获取权重的最大公约数
        /// </summary>
        /// <typeparam name="T">继承 IRoundRobinWeight 的权重对象</typeparam>
        /// <param name="list">权重轮询的抽取列表</param>
        /// <returns>返回最大公约数</returns>
        private static async Task<int> GetGcdWeightAsync<T>(List<T> list) where T : IWeightSecheduling
        {
            var min = list.Min(e => e.Weight);

            var gcd = 1;

            for (var i = 1; i <= min; i++)
            {
                if (list.Count(e => e.Weight % i != 0) == 0)
                {
                    gcd = i;
                }
            }

            return await Task.FromResult
            (
                result: gcd
            );
        }
    }
}
