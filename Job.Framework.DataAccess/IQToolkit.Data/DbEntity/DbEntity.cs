using IQToolkit.Data.Common;
using Job.Framework.Common;
using Job.Framework.DataAccess;
using System;

namespace IQToolkit.Data
{
    public abstract class DbEntity : DbContextBase
    {
        /// <summary>
        /// 初始化表结构对象
        /// </summary>
        protected override void OnCreateEntity(DbContextOptins options)
        {
            foreach (var item in ReflectionHelper.GetProperties(this))
            {
                if (ReflectionHelper.IsGenericType(item.PropertyType, typeof(IEntity<>)) == false)
                {
                    throw new InvalidCastException($"类型 { nameof(item.PropertyType) } 必须继承 { typeof(IEntity<>) }");
                }

                var mapping = DbTypeSystem.GetMapping(this.GetType(), item);
                var provider = DbTypeSystem.GetProvider(options.DbConnection, mapping, QueryPolicy.Default);

                item.CreateSetPropertyLambda()(this, provider.GetTable(ReflectionHelper.GetGenericType(item.PropertyType), item.Name));
            }
        }
    }
}