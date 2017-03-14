using Job.Framework.Interface;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Job.Framework.Common
{
    /// <summary>
    /// 权重随机调度算法帮助类
    /// </summary>
    public static class WeightRandomHelper
    {
        /// <summary>
        /// 根据得到新的权重排序值进行排序，取前面最大几个
        /// </summary>
        /// <typeparam name="T">继承 IRoundRobinWeight 的权重对象</typeparam>
        /// <param name="list">随机抽取记录的列表</param>
        /// <param name="count">抽取数量</param>
        /// <returns>返回抽取结果</returns>
        public static async Task<List<T>> GetAsync<T>(List<T> list, int count) where T : IWeightSecheduling
        {
            if (list == null || list.Count <= count || count <= 0)
            {
                return await Task.FromResult
                (
                    result: list
                );
            }

            var totalWeights = 0;  //计算权重总和

            for (var i = 0; i < list.Count; i++)
            {
                totalWeights += list[i].Weight + 1;  //权重+1，防止为 0 情况
            }

            var wlist = new List<KeyValuePair<int, int>>();  //第一个int为list下标索引、第一个int为权重排序值

            for (var i = 0; i < list.Count; i++)
            {
                wlist.Add(new KeyValuePair<int, int>(i, (list[i].Weight + 1) + await RandomHelper.NextAsync(0, totalWeights)));  // （权重+1） + 从0到（总权重-1）的随机数
            }

            wlist.Sort((kvp1, kvp2) =>
            {
                return kvp2.Value - kvp1.Value;
            });

            var newList = new List<T>();  //根据实际情况取排在最前面的几个

            for (var i = 0; i < count; i++)
            {
                newList.Add(list[wlist[i].Key]);
            }

            return await Task.FromResult
            (
                result: newList
            );
        }
    }
}
