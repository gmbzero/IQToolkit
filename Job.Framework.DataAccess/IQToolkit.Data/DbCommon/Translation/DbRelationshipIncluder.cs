using System.Linq.Expressions;
using System.Reflection;

namespace IQToolkit.Data.Common
{
    internal class DbRelationshipIncluder : DbExpressionVisitor
    {
        private readonly QueryMapper mapper;
        private readonly QueryPolicy policy;
        private ScopedDictionary<MemberInfo, bool> includeScope;

        private DbRelationshipIncluder(QueryMapper mapper)
        {
            this.mapper = mapper;
            this.policy = mapper.Translator.Police.Policy;
            this.includeScope = new ScopedDictionary<MemberInfo, bool>(null);
        }

        public static Expression Include(QueryMapper mapper, Expression expression)
        {
            return new DbRelationshipIncluder(mapper).Visit(expression);
        }

        protected override Expression VisitProjection(DbProjectionExpression proj)
        {
            return this.UpdateProjection(proj, proj.Select, this.Visit(proj.Projector), proj.Aggregator);
        }

        protected override Expression VisitEntity(DbEntityExpression entity)
        {
            var save = this.includeScope;

            this.includeScope = new ScopedDictionary<MemberInfo, bool>(this.includeScope);

            try
            {
                if (this.mapper.HasIncludedMembers(entity))
                {
                    entity = this.mapper.IncludeMembers(entity, m =>
                    {
                        if (this.includeScope.ContainsKey(m))
                        {
                            return false;
                        }

                        if (this.policy.IsIncluded(m))
                        {
                            this.includeScope.Add(m, true);

                            return true;
                        }

                        return false;
                    });
                }

                return base.VisitEntity(entity);
            }
            finally
            {
                this.includeScope = save;
            }
        }
    }
}