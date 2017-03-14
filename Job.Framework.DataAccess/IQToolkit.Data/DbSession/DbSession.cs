using IQToolkit.Data.Common;
using Job.Framework.Common;
using Job.Framework.DataAccess;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace IQToolkit.Data
{
    public abstract class DbSession : DbContextBase
    {
        protected override void OnCreateEntity(DbContextOptins options)
        {
            foreach (var item in ReflectionHelper.GetProperties(this))
            {
                if (ReflectionHelper.IsGenericType(item.PropertyType, typeof(ISession<>)) == false)
                {
                    throw new InvalidCastException($"类型 { nameof(item.PropertyType) } 必须继承 { typeof(ISession<>) }");
                }

                var mapping = DbTypeSystem.GetMapping(this.GetType(), item);
                var provider = new DbSessionProvider(DbTypeSystem.GetProvider(options.DbConnection, mapping, new DbEntityPolicy()));

                item.CreateSetPropertyLambda()(this, provider.GetTable(ReflectionHelper.GetGenericType(item.PropertyType), item.Name));
            }
        }
    }
}