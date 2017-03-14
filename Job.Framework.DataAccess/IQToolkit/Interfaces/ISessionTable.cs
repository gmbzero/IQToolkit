using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace IQToolkit
{
    public enum SubmitAction
    {
        None,
        Update,
        PossibleUpdate,
        Insert,
        InsertOrUpdate,
        Delete
    }

    public interface ISessionTable
    {
        IEntityProvider Provider { get; }
        ISession<T> GetTable<T>(string tableId);
        ISession GetTable(Type elementType, string tableId);
        void SubmitChanges();
        Task SubmitChangesAsync(CancellationToken cancellationToken = default(CancellationToken));
    }
}