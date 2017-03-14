using System;
using System.Linq;
using System.Linq.Expressions;

namespace IQToolkit
{
    public interface IEntityProvider : IQueryProvider
    {
        IEntity<T> GetTable<T>(string tableId);
        IEntity GetTable(Type type, string tableId);
        bool CanBeEvaluatedLocally(Expression expression);
        bool CanBeParameter(Expression expression);
    }
}