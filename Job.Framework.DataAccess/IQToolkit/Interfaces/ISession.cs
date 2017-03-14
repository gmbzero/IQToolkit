using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace IQToolkit
{
    public interface ISession : IQueryable
    {
        ISessionTable Session { get; }
        IEntity ProviderTable { get; }
        object GetById(object id);
        void SetSubmitAction(object instance, SubmitAction action);
        SubmitAction GetSubmitAction(object instance);
    }

    public interface ISession<T> : IQueryable<T>, ISession, IEnumerableAsync<T>
    {
        new IEntity<T> ProviderTable { get; }
        new T GetById(object id);
        void SetSubmitAction(T instance, SubmitAction action);
        SubmitAction GetSubmitAction(T instance);
    }
}
