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
    public class DbSessionProvider : ISessionTable
    {
        private readonly EntityProvider provider;
        private readonly SessionProvider sessionProvider;
        private static ConcurrentDictionary<MappingEntity, ISession> tables = new ConcurrentDictionary<MappingEntity, ISession>();

        IEntityProvider ISessionTable.Provider
        {
            get { return this.Provider; }
        }

        public IEntityProvider Provider
        {
            get { return this.sessionProvider; }
        }

        protected IEnumerable<ISession> GetTables()
        {
            return tables.Values;
        }

        internal DbSessionProvider(EntityProvider provider)
        {
            this.provider = provider;
            this.sessionProvider = new SessionProvider(this, provider);
        }

        public ISession GetTable(Type elementType, string tableId)
        {
            return this.GetTable(this.sessionProvider.Provider.Mapping.GetEntity(elementType, tableId));
        }

        public ISession<T> GetTable<T>(string tableId)
        {
            return this.GetTable(typeof(T), tableId) as ISession<T>;
        }

        internal ISession GetTable(MappingEntity entity)
        {
            return tables.GetOrAdd(entity, this.CreateTable(entity));
        }

        private object OnEntityMaterialized(MappingEntity entity, object instance)
        {
            var table = this.GetTable(entity) as IEntitySessionTable;

            return table.OnEntityMaterialized(instance);
        }

        private interface IEntitySessionTable : ISession
        {
            object OnEntityMaterialized(object instance);

            MappingEntity Entity { get; }
        }

        private abstract class SessionTable<T> : Query<T>, ISession<T>, ISession, IEntitySessionTable
        {
            private readonly DbSessionProvider session;
            private readonly MappingEntity entity;
            private readonly IEntity<T> underlyingTable;

            public ISessionTable Session
            {
                get { return this.session; }
            }

            public MappingEntity Entity
            {
                get { return this.entity; }
            }

            public IEntity<T> ProviderTable
            {
                get { return this.underlyingTable; }
            }

            IEntity ISession.ProviderTable
            {
                get { return this.underlyingTable; }
            }

            public SessionTable(DbSessionProvider session, MappingEntity entity) : base(session.sessionProvider, typeof(ISession<T>))
            {
                this.session = session;
                this.entity = entity;
                this.underlyingTable = this.session.Provider.GetTable<T>(entity.TableId);
            }

            public T GetById(object id)
            {
                return this.underlyingTable.GetById(id);
            }

            object ISession.GetById(object id)
            {
                return this.GetById(id);
            }

            public virtual object OnEntityMaterialized(object instance)
            {
                return instance;
            }

            public virtual void SetSubmitAction(T instance, SubmitAction action)
            {
                throw new NotImplementedException();
            }

            void ISession.SetSubmitAction(object instance, SubmitAction action)
            {
                this.SetSubmitAction((T)instance, action);
            }

            public virtual SubmitAction GetSubmitAction(T instance)
            {
                throw new NotImplementedException();
            }

            SubmitAction ISession.GetSubmitAction(object instance)
            {
                return this.GetSubmitAction((T)instance);
            }
        }

        private class SessionProvider : QueryProvider, IEntityProvider, ICreateExecutor
        {
            private readonly DbSessionProvider session;
            private readonly EntityProvider provider;

            public EntityProvider Provider
            {
                get { return this.provider; }
            }

            public SessionProvider(DbSessionProvider session, EntityProvider provider)
            {
                this.session = session;
                this.provider = provider;
            }

            public override object Execute(Expression expression)
            {
                return this.provider.Execute(expression);
            }

            public override string GetQueryText(Expression expression)
            {
                return this.provider.GetQueryText(expression);
            }

            public IEntity<T> GetTable<T>(string tableId)
            {
                return this.provider.GetTable<T>(tableId);
            }

            public IEntity GetTable(Type type, string tableId)
            {
                return this.provider.GetTable(type, tableId);
            }

            public bool CanBeEvaluatedLocally(Expression expression)
            {
                return this.provider.Mapping.CanBeEvaluatedLocally(expression);
            }

            public bool CanBeParameter(Expression expression)
            {
                return this.provider.CanBeParameter(expression);
            }

            QueryExecutor ICreateExecutor.CreateExecutor()
            {
                return new SessionExecutor(this.session, (this.provider as ICreateExecutor).CreateExecutor());
            }
        }

        private class SessionExecutor : QueryExecutor
        {
            private readonly DbSessionProvider session;
            private readonly QueryExecutor executor;

            public SessionExecutor(DbSessionProvider session, QueryExecutor executor)
            {
                this.session = session;
                this.executor = executor;
            }

            public override int RowsAffected
            {
                get { return this.executor.RowsAffected; }
            }

            public override object Convert(object value, Type type)
            {
                return this.executor.Convert(value, type);
            }

            public override IEnumerable<T> Execute<T>(QueryCommand command, Func<FieldReader, T> fnProjector, MappingEntity entity, object[] paramValues)
            {
                return this.executor.Execute<T>(command, Wrap(fnProjector, entity), entity, paramValues);
            }

            public override IEnumerable<int> ExecuteBatch(QueryCommand query, IEnumerable<object[]> paramSets, int batchSize, bool stream)
            {
                return this.executor.ExecuteBatch(query, paramSets, batchSize, stream);
            }

            public override IEnumerable<T> ExecuteBatch<T>(QueryCommand query, IEnumerable<object[]> paramSets, Func<FieldReader, T> fnProjector, MappingEntity entity, int batchSize, bool stream)
            {
                return this.executor.ExecuteBatch<T>(query, paramSets, Wrap(fnProjector, entity), entity, batchSize, stream);
            }

            public override IEnumerable<T> ExecuteDeferred<T>(QueryCommand query, Func<FieldReader, T> fnProjector, MappingEntity entity, object[] paramValues)
            {
                return this.executor.ExecuteDeferred<T>(query, Wrap(fnProjector, entity), entity, paramValues);
            }

            public override int ExecuteCommand(QueryCommand query, object[] paramValues)
            {
                return this.executor.ExecuteCommand(query, paramValues);
            }

            private Func<FieldReader, T> Wrap<T>(Func<FieldReader, T> fnProjector, MappingEntity entity)
            {
                if (entity == null)
                {
                    return new Func<FieldReader, T>((fr) => fnProjector(fr));
                }

                return new Func<FieldReader, T>((fr) => (T)this.session.OnEntityMaterialized(entity, fnProjector(fr)));
            }
        }

        public virtual void SubmitChanges()
        {
            this.provider.DoTransacted(new Action(() =>
            {
                var submitted = new List<TrackedItem>();

                foreach (var item in this.GetOrderedItems())
                {
                    if (item.Table.SubmitChanges(item))
                    {
                        submitted.Add(item);
                    }
                }

                foreach (var item in submitted)
                {
                    item.Table.AcceptChanges(item);
                }
            }));
        }

        public virtual Task SubmitChangesAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            return this.provider.DoTransactedAsync(new Func<CancellationToken, Task>(async (ct) =>
            {
                var submitted = new List<TrackedItem>();

                foreach (var item in this.GetOrderedItems())
                {
                    if (await item.Table.SubmitChangesAsync(item, ct).ConfigureAwait(false))
                    {
                        submitted.Add(item);
                    }
                }

                foreach (var item in submitted)
                {
                    item.Table.AcceptChanges(item);
                }

            }), cancellationToken);
        }

        internal virtual ISession CreateTable(MappingEntity entity)
        {
            return Activator.CreateInstance(typeof(TrackedTable<>).MakeGenericType(entity.ElementType), new object[] { this, entity }) as ISession;
        }

        private interface ITrackedTable : IEntitySessionTable
        {
            object GetFromCacheById(object key);
            IEnumerable<TrackedItem> TrackedItems { get; }
            TrackedItem GetTrackedItem(object instance);
            bool SubmitChanges(TrackedItem item);
            Task<bool> SubmitChangesAsync(TrackedItem item, CancellationToken cancellationToken);
            void AcceptChanges(TrackedItem item);
        }

        private class TrackedTable<T> : SessionTable<T>, ITrackedTable
        {
            private readonly Dictionary<T, TrackedItem> tracked;
            private readonly Dictionary<object, T> identityCache;

            public TrackedTable(DbSessionProvider session, MappingEntity entity) : base(session, entity)
            {
                this.tracked = new Dictionary<T, TrackedItem>();
                this.identityCache = new Dictionary<object, T>();
            }

            IEnumerable<TrackedItem> ITrackedTable.TrackedItems
            {
                get { return this.tracked.Values; }
            }

            TrackedItem ITrackedTable.GetTrackedItem(object instance)
            {
                if (this.tracked.TryGetValue((T)instance, out TrackedItem ti))
                {
                    return ti;
                }

                return null;
            }

            object ITrackedTable.GetFromCacheById(object key)
            {
                this.identityCache.TryGetValue(key, out T cached);

                return cached;
            }

            public bool SubmitChanges(TrackedItem item)
            {
                switch (item.State)
                {
                    case SubmitAction.Delete:
                        {
                            this.ProviderTable.Delete((T)item.Instance);

                            return true;
                        }
                    case SubmitAction.Insert:
                        {
                            this.ProviderTable.Insert((T)item.Instance);

                            return true;
                        }
                    case SubmitAction.InsertOrUpdate:
                        {
                            this.ProviderTable.InsertOrUpdate((T)item.Instance);

                            return true;
                        }
                    case SubmitAction.PossibleUpdate:
                        {
                            if (item.Original != null && this.Mapping.IsModified(item.Entity, item.Instance, item.Original))
                            {
                                this.ProviderTable.Update((T)item.Instance);

                                return true;
                            }

                            break;
                        }
                    case SubmitAction.Update:
                        {
                            this.ProviderTable.Update((T)item.Instance);

                            return true;
                        }
                    default:
                        {
                            break; // do nothing
                        }
                }

                return false;
            }

            public async Task<bool> SubmitChangesAsync(TrackedItem item, CancellationToken cancellationToken)
            {
                switch (item.State)
                {
                    case SubmitAction.Delete:
                        {
                            await this.ProviderTable.DeleteAsync((T)item.Instance, cancellationToken).ConfigureAwait(false);

                            return true;
                        }
                    case SubmitAction.Insert:
                        {
                            await this.ProviderTable.InsertAsync((T)item.Instance, cancellationToken).ConfigureAwait(false);

                            return true;
                        }
                    case SubmitAction.InsertOrUpdate:
                        {
                            await this.ProviderTable.InsertOrUpdateAsync((T)item.Instance, cancellationToken).ConfigureAwait(false);

                            return true;
                        }
                    case SubmitAction.PossibleUpdate:
                        {
                            if (item.Original != null && this.Mapping.IsModified(item.Entity, item.Instance, item.Original))
                            {
                                await this.ProviderTable.UpdateAsync((T)item.Instance, cancellationToken).ConfigureAwait(false);

                                return true;
                            }
                            break;
                        }
                    case SubmitAction.Update:
                        {
                            await this.ProviderTable.UpdateAsync((T)item.Instance, cancellationToken).ConfigureAwait(false);

                            return true;
                        }
                    default:
                        {
                            break; // do nothing
                        }
                }

                return false;
            }

            private void AcceptChanges(TrackedItem item)
            {
                switch (item.State)
                {
                    case SubmitAction.Delete:
                        {
                            this.RemoveFromCache((T)item.Instance);
                            this.AssignAction((T)item.Instance, SubmitAction.None);

                            break;
                        }
                    case SubmitAction.Insert:
                        {
                            this.AddToCache((T)item.Instance);
                            this.AssignAction((T)item.Instance, SubmitAction.PossibleUpdate);

                            break;
                        }
                    case SubmitAction.InsertOrUpdate:
                        {
                            this.AddToCache((T)item.Instance);
                            this.AssignAction((T)item.Instance, SubmitAction.PossibleUpdate);

                            break;
                        }
                    case SubmitAction.PossibleUpdate:
                    case SubmitAction.Update:
                        {
                            this.AssignAction((T)item.Instance, SubmitAction.PossibleUpdate);

                            break;
                        }
                    default:
                        {
                            break; // do nothing
                        }
                }
            }

            void ITrackedTable.AcceptChanges(TrackedItem item)
            {
                this.AcceptChanges(item);
            }

            public override object OnEntityMaterialized(object instance)
            {
                var typedInstance = (T)instance;
                var cached = this.AddToCache(typedInstance);

                if ((object)cached == (object)typedInstance)
                {
                    this.AssignAction(typedInstance, SubmitAction.PossibleUpdate);
                }

                return cached;
            }

            public override SubmitAction GetSubmitAction(T instance)
            {
                if (this.tracked.TryGetValue(instance, out TrackedItem ti))
                {
                    if (ti.State == SubmitAction.PossibleUpdate)
                    {
                        if (this.Mapping.IsModified(ti.Entity, ti.Instance, ti.Original))
                        {
                            return SubmitAction.Update;
                        }
                        else
                        {
                            return SubmitAction.None;
                        }
                    }

                    return ti.State;
                }

                return SubmitAction.None;
            }

            public override void SetSubmitAction(T instance, SubmitAction action)
            {
                switch (action)
                {
                    case SubmitAction.None:
                    case SubmitAction.PossibleUpdate:
                    case SubmitAction.Update:
                    case SubmitAction.Delete:
                        {
                            var cached = this.AddToCache(instance);

                            if ((object)cached != (object)instance)
                            {
                                throw new InvalidOperationException("An different instance with the same key is already in the cache.");
                            }

                            break;
                        }
                }

                this.AssignAction(instance, action);
            }

            private QueryMapping Mapping
            {
                get { return (this.Session as DbSessionProvider).provider.Mapping; }
            }

            private T AddToCache(T instance)
            {
                var key = this.Mapping.GetPrimaryKey(this.Entity, instance);

                if (this.identityCache.TryGetValue(key, out T cached) == false)
                {
                    cached = instance;

                    this.identityCache.Add(key, cached);
                }

                return cached;
            }

            private void RemoveFromCache(T instance)
            {
                var key = this.Mapping.GetPrimaryKey(this.Entity, instance);

                this.identityCache.Remove(key);
            }

            private void AssignAction(T instance, SubmitAction action)
            {
                this.tracked.TryGetValue(instance, out TrackedItem ti);

                switch (action)
                {
                    case SubmitAction.Insert:
                    case SubmitAction.InsertOrUpdate:
                    case SubmitAction.Update:
                    case SubmitAction.Delete:
                    case SubmitAction.None:
                        {
                            this.tracked[instance] = new TrackedItem(this, instance, ti?.Original, action, ti != null ? ti.HookedEvent : false);

                            break;
                        }
                    case SubmitAction.PossibleUpdate:
                        {
                            if (instance is INotifyPropertyChanging notify)
                            {
                                if (!ti.HookedEvent)
                                {
                                    notify.PropertyChanging += new PropertyChangingEventHandler(this.OnPropertyChanging);
                                }

                                this.tracked[instance] = new TrackedItem(this, instance, null, SubmitAction.PossibleUpdate, true);
                            }
                            else
                            {
                                var original = this.Mapping.CloneEntity(this.Entity, instance);

                                this.tracked[instance] = new TrackedItem(this, instance, original, SubmitAction.PossibleUpdate, false);
                            }

                            break;
                        }
                    default:
                        {
                            throw new InvalidOperationException($"Unknown SubmitAction: { action }");
                        }
                }
            }

            protected virtual void OnPropertyChanging(object sender, PropertyChangingEventArgs args)
            {
                if (this.tracked.TryGetValue((T)sender, out TrackedItem ti) && ti.State == SubmitAction.PossibleUpdate)
                {
                    var clone = this.Mapping.CloneEntity(ti.Entity, ti.Instance);

                    this.tracked[(T)sender] = new TrackedItem(this, ti.Instance, clone, SubmitAction.Update, true);
                }
            }
        }

        private class TrackedItem
        {
            private readonly ITrackedTable table;
            private readonly object instance;
            private readonly object original;
            private readonly SubmitAction state;
            private readonly bool hookedEvent;
            public static readonly IEnumerable<TrackedItem> EmptyList = new TrackedItem[] { };

            public ITrackedTable Table
            {
                get { return this.table; }
            }

            public MappingEntity Entity
            {
                get { return this.table.Entity; }
            }

            public object Instance
            {
                get { return this.instance; }
            }

            public object Original
            {
                get { return this.original; }
            }

            public SubmitAction State
            {
                get { return this.state; }
            }

            public bool HookedEvent
            {
                get { return this.hookedEvent; }
            }

            public TrackedItem(ITrackedTable table, object instance, object original, SubmitAction state, bool hookedEvent)
            {
                this.table = table;
                this.instance = instance;
                this.original = original;
                this.state = state;
                this.hookedEvent = hookedEvent;
            }
        }

        private IEnumerable<TrackedItem> GetOrderedItems()
        {
            var items =
            (
                from tab in this.GetTables()
                from ti in ((ITrackedTable)tab).TrackedItems
                where ti.State != SubmitAction.None
                select ti
            ).ToList();

            var edges = this.GetEdges(items).Distinct().ToList();
            var stLookup = edges.ToLookup(e => e.Source, e => e.Target);
            var tsLookup = edges.ToLookup(e => e.Target, e => e.Source);

            return TopologicalSorter.Sort(items, item =>
            {
                switch (item.State)
                {
                    case SubmitAction.Insert:
                    case SubmitAction.InsertOrUpdate:
                        {
                            var beforeMe = stLookup[item];
                            var cached = item.Table.GetFromCacheById(this.provider.Mapping.GetPrimaryKey(item.Entity, item.Instance));

                            if (cached != null && cached != item.Instance)
                            {
                                var ti = item.Table.GetTrackedItem(cached);

                                if (ti != null && ti.State == SubmitAction.Delete)
                                {
                                    beforeMe = beforeMe.Concat(new[] { ti });
                                }
                            }

                            return beforeMe;
                        }
                    case SubmitAction.Delete:
                        {
                            return tsLookup[item];
                        }
                    default:
                        {
                            return TrackedItem.EmptyList;
                        }
                }
            });
        }

        private TrackedItem GetTrackedItem(EntityInfo entity)
        {
            var table = this.GetTable(entity.Mapping) as ITrackedTable;

            return table.GetTrackedItem(entity.Instance);
        }

        private IEnumerable<Edge> GetEdges(IEnumerable<TrackedItem> items)
        {
            foreach (var c in items)
            {
                foreach (var d in this.provider.Mapping.GetDependingEntities(c.Entity, c.Instance))
                {
                    var dc = this.GetTrackedItem(d);

                    if (dc != null)
                    {
                        yield return new Edge(dc, c);
                    }
                }

                foreach (var d in this.provider.Mapping.GetDependentEntities(c.Entity, c.Instance))
                {
                    var dc = this.GetTrackedItem(d);

                    if (dc != null)
                    {
                        yield return new Edge(c, dc);
                    }
                }
            }
        }

        private class Edge : IEquatable<Edge>
        {
            private int hash;
            public TrackedItem Source { get; }
            public TrackedItem Target { get; }

            public Edge(TrackedItem source, TrackedItem target)
            {
                this.Source = source;
                this.Target = target;

                this.hash = this.Source.GetHashCode() + this.Target.GetHashCode();
            }

            public bool Equals(Edge edge)
            {
                return edge != null && this.Source == edge.Source && this.Target == edge.Target;
            }

            public override bool Equals(object obj)
            {
                return this.Equals(obj as Edge);
            }

            public override int GetHashCode()
            {
                return this.hash;
            }
        }
    }
}
