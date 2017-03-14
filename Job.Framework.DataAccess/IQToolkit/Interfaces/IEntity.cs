using System.Linq;

namespace IQToolkit
{
    public interface IEntity : IQueryable, IUpdatable
    {
        new IEntityProvider Provider { get; }
        string TableId { get; }
        object GetById(object id);
    }

    public interface IEntity<T> : IQueryable<T>, IEntity, IUpdatable<T>, IEnumerableAsync<T>
    {
        new T GetById(object id);
    }
}
