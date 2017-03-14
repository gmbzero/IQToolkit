using IQToolkit.Data.Common;
using IQToolkit.Data.Mapping;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace IQToolkit.Data
{
    internal abstract class EntityProvider : QueryProvider, IEntityProvider, ICreateExecutor
    {
        private TextWriter log;
        private QueryPolicy policy;
        private readonly QueryLanguage language;
        private readonly QueryMapping mapping;
        private static ConcurrentDictionary<MappingEntity, IEntity> tables = new ConcurrentDictionary<MappingEntity, IEntity>();

        public QueryMapping Mapping
        {
            get { return this.mapping; }
        }

        public QueryLanguage Language
        {
            get { return this.language; }
        }

        public QueryPolicy Policy
        {
            get { return this.policy; }

            set
            {
                if (value == null)
                {
                    this.policy = QueryPolicy.Default;
                }
                else
                {
                    this.policy = value;
                }
            }
        }

        public TextWriter Log
        {
            get { return this.log; }
            set { this.log = value; }
        }

        public EntityProvider(QueryLanguage language, QueryMapping mapping, QueryPolicy policy)
        {
            this.language = language ?? throw new InvalidOperationException("Language not specified");
            this.mapping = mapping ?? throw new InvalidOperationException("Mapping not specified");
            this.policy = policy ?? throw new InvalidOperationException("Policy not specified");
        }

        public IEntity GetTable(MappingEntity entity)
        {
            return tables.GetOrAdd(entity, this.CreateTable(entity));
        }

        protected virtual IEntity CreateTable(MappingEntity entity)
        {
            return Activator.CreateInstance(typeof(EntityTable<>).MakeGenericType(entity.ElementType), new object[] { this, entity }) as IEntity;
        }

        public virtual IEntity<T> GetTable<T>()
        {
            return GetTable<T>(null);
        }

        public virtual IEntity<T> GetTable<T>(string tableId)
        {
            return this.GetTable(typeof(T), tableId) as IEntity<T>;
        }

        public virtual IEntity GetTable(Type type)
        {
            return GetTable(type, null);
        }

        public virtual IEntity GetTable(Type type, string tableId)
        {
            return this.GetTable(this.Mapping.GetEntity(type, tableId));
        }

        public bool CanBeEvaluatedLocally(Expression expression)
        {
            return this.Mapping.CanBeEvaluatedLocally(expression);
        }

        public virtual bool CanBeParameter(Expression expression)
        {
            var type = TypeHelper.GetNonNullableType(expression.Type);

            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Object:
                    {
                        if (expression.Type == typeof(Byte[]) || expression.Type == typeof(Char[]))
                        {
                            return true;
                        }

                        return false;
                    }
                default:
                    {
                        return true;
                    }
            }
        }

        QueryExecutor ICreateExecutor.CreateExecutor()
        {
            return this.CreateExecutor();
        }

        protected abstract QueryExecutor CreateExecutor();

        internal class EntityTable<T> : Query<T>, IEntity<T>, IHaveMappingEntity
        {
            private readonly MappingEntity entity;
            private readonly EntityProvider provider;

            public MappingEntity Entity
            {
                get { return this.entity; }
            }

            new public IEntityProvider Provider
            {
                get { return this.provider; }
            }

            public string TableId
            {
                get { return this.entity.TableId; }
            }

            public Type EntityType
            {
                get { return this.entity.EntityType; }
            }

            public EntityTable(EntityProvider provider, MappingEntity entity) : base(provider, typeof(IEntity<T>))
            {
                this.provider = provider;
                this.entity = entity;
            }

            public T GetById(object id)
            {
                var dbProvider = this.Provider;

                if (dbProvider != null)
                {
                    var keys = id as IEnumerable<object>;

                    if (keys == null)
                    {
                        keys = new object[] { id };
                    }

                    var query = (dbProvider as EntityProvider).Mapping.GetPrimaryKeyQuery(this.entity, this.Expression, keys.Select(v => Expression.Constant(v)).ToArray());

                    return this.Provider.Execute<T>(query);
                }

                return default(T);
            }

            object IEntity.GetById(object id)
            {
                return this.GetById(id);
            }
        }

        public override string GetQueryText(Expression expression)
        {
            var plan = this.GetExecutionPlan(expression);
            var commands = CommandGatherer.Gather(plan).Select(c => c.CommandText).ToArray();

            return string.Join("\n\n", commands);
        }

        private class CommandGatherer : DbExpressionVisitor
        {
            private readonly List<QueryCommand> commands;

            public CommandGatherer()
            {
                this.commands = new List<QueryCommand>();
            }

            public static ReadOnlyCollection<QueryCommand> Gather(Expression expression)
            {
                var gatherer = new CommandGatherer();

                if (gatherer != null)
                {
                    gatherer.Visit(expression);
                }

                return gatherer.commands.AsReadOnly();
            }

            protected override Expression VisitConstant(ConstantExpression c)
            {
                if (c.Value is QueryCommand qc)
                {
                    this.commands.Add(qc);
                }

                return c;
            }
        }

        public string GetQueryPlan(Expression expression)
        {
            var plan = this.GetExecutionPlan(expression);

            return DbExpressionWriter.WriteToString(this.Language, plan);
        }

        protected virtual QueryTranslator CreateTranslator()
        {
            return new QueryTranslator(this.language, this.mapping, this.policy);
        }

        public abstract void DoTransacted(Action action);
        public abstract Task DoTransactedAsync(Func<CancellationToken, Task> asyncAction, CancellationToken cancellationToken);
        public abstract void DoConnected(Action action);
        public abstract Task DoConnectedAsync(Func<CancellationToken, Task> asyncAction, CancellationToken cancellationToken);
        public abstract int ExecuteCommand(string commandText);
        public abstract Task<int> ExecuteCommandAsync(string commandText, CancellationToken cancellationToken);

        public override object Execute(Expression expression)
        {
            var plan = this.GetExecutionPlan(expression);

#if DEBUG
            Debug.WriteLine(plan.ToString());
#endif

            if (expression is LambdaExpression lambda)
            {
                var fn = Expression.Lambda(lambda.Type, plan, lambda.Parameters);

                return fn.Compile();
            }
            else
            {
                var efn = Expression.Lambda<Func<object>>(Expression.Convert(plan, typeof(object)));
                var fn = efn.Compile();

                return fn();
            }
        }

        public virtual Expression GetExecutionPlan(Expression expression)
        {
            var lambda = expression as LambdaExpression;

            if (lambda != null)
            {
                expression = lambda.Body;
            }

            var translator = this.CreateTranslator();
            var translation = translator.Translate(expression);
            var parameters = lambda?.Parameters;
            var providerExpression = this.Find(expression, parameters, typeof(EntityProvider));

            if (providerExpression == null)
            {
                providerExpression = Expression.Property
                (
                    this.Find(expression, parameters, typeof(IQueryable)),
                    typeof(IQueryable).GetProperty(nameof(IQueryable.Provider))
                );
            }

            return translator.Police.BuildExecutionPlan(translation, providerExpression);
        }

        private Expression Find(Expression expression, IList<ParameterExpression> parameters, Type type)
        {
            if (parameters != null)
            {
                var found = parameters.FirstOrDefault(p => type.IsAssignableFrom(p.Type));

                if (found != null)
                {
                    return found;
                }
            }

            return ExpressionSubFinder.Find(expression, type);
        }
    }
}