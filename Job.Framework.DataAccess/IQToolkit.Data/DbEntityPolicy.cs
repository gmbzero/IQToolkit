using IQToolkit.Data.Common;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace IQToolkit.Data
{
    internal class DbEntityPolicy : QueryPolicy
    {
        private readonly HashSet<MemberInfo> included;
        private readonly HashSet<MemberInfo> deferred;
        private readonly Dictionary<MemberInfo, List<LambdaExpression>> operations;

        public DbEntityPolicy()
        {
            this.included = new HashSet<MemberInfo>();
            this.deferred = new HashSet<MemberInfo>();
            this.operations = new Dictionary<MemberInfo, List<LambdaExpression>>();
        }

        public void Apply(LambdaExpression fnApply)
        {
            if (fnApply == null)
            {
                throw new ArgumentNullException("fnApply");
            }

            if (fnApply.Parameters.Count != 1)
            {
                throw new ArgumentException("Apply function has wrong number of arguments.");
            }

            this.AddOperation(TypeHelper.GetElementType(fnApply.Parameters[0].Type).GetTypeInfo(), fnApply);
        }

        public void Apply<TEntity>(Expression<Func<IEnumerable<TEntity>, IEnumerable<TEntity>>> fnApply)
        {
            Apply(fnApply as LambdaExpression);
        }

        public void Include(MemberInfo member)
        {
            Include(member, false);
        }

        public void Include(MemberInfo member, bool deferLoad)
        {
            this.included.Add(member);

            if (deferLoad)
            {
                Defer(member);
            }
        }

        public void IncludeWith(LambdaExpression fnMember)
        {
            IncludeWith(fnMember, false);
        }

        public void IncludeWith(LambdaExpression fnMember, bool deferLoad)
        {
            var rootMember = RootMemberFinder.Find(fnMember, fnMember.Parameters[0]);

            if (rootMember == null)
            {
                throw new InvalidOperationException("Subquery does not originate with a member access");
            }

            Include(rootMember.Member, deferLoad);

            if (rootMember != fnMember.Body)
            {
                AssociateWith(fnMember);
            }
        }

        public void IncludeWith<TEntity>(Expression<Func<TEntity, object>> fnMember)
        {
            IncludeWith(fnMember as LambdaExpression, false);
        }

        public void IncludeWith<TEntity>(Expression<Func<TEntity, object>> fnMember, bool deferLoad)
        {
            IncludeWith(fnMember as LambdaExpression, deferLoad);
        }

        private void Defer(MemberInfo member)
        {
            var mType = TypeHelper.GetMemberType(member);

            if (mType.GetTypeInfo().IsGenericType)
            {
                var gType = mType.GetGenericTypeDefinition();

                if (gType != typeof(IEnumerable<>) && gType != typeof(IList<>) && !typeof(IDeferLoadable).IsAssignableFrom(mType))
                {
                    throw new InvalidOperationException($"The member '{ member }' cannot be deferred due to its type.");
                }
            }

            this.deferred.Add(member);
        }

        public void AssociateWith(LambdaExpression memberQuery)
        {
            var rootMember = RootMemberFinder.Find(memberQuery, memberQuery.Parameters[0]);

            if (rootMember == null)
            {
                throw new InvalidOperationException("Subquery does not originate with a member access");
            }

            if (rootMember != memberQuery.Body)
            {
                var memberParam = Expression.Parameter(rootMember.Type, "root");
                var newBody = ExpressionReplacer.Replace(memberQuery.Body, rootMember, memberParam);

                this.AddOperation(rootMember.Member, Expression.Lambda(newBody, memberParam));
            }
        }

        private void AddOperation(MemberInfo member, LambdaExpression operation)
        {
            if (!this.operations.TryGetValue(member, out List<LambdaExpression> memberOps))
            {
                memberOps = new List<LambdaExpression>();

                this.operations.Add(member, memberOps);
            }

            memberOps.Add(operation);
        }

        public void AssociateWith<TEntity>(Expression<Func<TEntity, IEnumerable>> memberQuery)
        {
            AssociateWith(memberQuery as LambdaExpression);
        }

        private class RootMemberFinder : ExpressionVisitor
        {
            private MemberExpression found;
            private readonly ParameterExpression parameter;

            private RootMemberFinder(ParameterExpression parameter)
            {
                this.parameter = parameter;
            }

            public static MemberExpression Find(Expression query, ParameterExpression parameter)
            {
                var finder = new RootMemberFinder(parameter);

                if (finder != null)
                {
                    finder.Visit(query);
                }

                return finder.found;
            }

            protected override Expression VisitMethodCall(MethodCallExpression m)
            {
                if (m.Object != null)
                {
                    this.Visit(m.Object);
                }
                else if (m.Arguments.Count > 0)
                {
                    this.Visit(m.Arguments[0]);
                }

                return m;
            }

            protected override Expression VisitMemberAccess(MemberExpression m)
            {
                if (m.Expression == this.parameter)
                {
                    this.found = m;

                    return m;
                }
                else
                {
                    return base.VisitMemberAccess(m);
                }
            }
        }

        public override bool IsIncluded(MemberInfo member)
        {
            return this.included.Contains(member);
        }

        public override bool IsDeferLoaded(MemberInfo member)
        {
            return this.deferred.Contains(member);
        }

        public override QueryPolice CreatePolice(QueryTranslator translator)
        {
            return new Police(this, translator);
        }

        private class Police : QueryPolice
        {
            private readonly DbEntityPolicy policy;

            public Police(DbEntityPolicy policy, QueryTranslator translator) : base(policy, translator)
            {
                this.policy = policy;
            }

            public override Expression ApplyPolicy(Expression expression, MemberInfo member)
            {
                if (this.policy.operations.TryGetValue(member, out List<LambdaExpression> ops))
                {
                    var result = expression;

                    foreach (var fnOp in ops)
                    {
                        var pop = ExpressionEvaluator.Eval(fnOp, this.Translator.Mapper.Mapping.CanBeEvaluatedLocally);

                        result = this.Translator.Mapper.ApplyMapping(Expression.Invoke(pop, result));
                    }

                    var projection = result as DbProjectionExpression;

                    if (projection.Type != expression.Type)
                    {
                        var fnAgg = DbAggregator.GetAggregator(expression.Type, projection.Type);

                        projection = new DbProjectionExpression(projection.Select, projection.Projector, fnAgg);
                    }

                    return projection;
                }

                return expression;
            }
        }
    }
}