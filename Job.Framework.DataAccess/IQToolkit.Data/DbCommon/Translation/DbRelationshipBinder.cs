using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbRelationshipBinder : DbExpressionVisitor
    {
        private Expression currentFrom;
        private readonly QueryMapper mapper;
        private readonly QueryMapping mapping;
        private readonly QueryLanguage language;

        private DbRelationshipBinder(QueryMapper mapper)
        {
            this.mapper = mapper;
            this.mapping = mapper.Mapping;
            this.language = mapper.Translator.Linguist.Language;
        }

        public static Expression Bind(QueryMapper mapper, Expression expression)
        {
            return new DbRelationshipBinder(mapper).Visit(expression);
        }

        protected override Expression VisitSelect(DbSelectExpression select)
        {
            var saveCurrentFrom = this.currentFrom;

            this.currentFrom = this.VisitSource(select.From);

            try
            {
                var where = this.Visit(select.Where);
                var orderBy = this.VisitOrderBy(select.OrderBy);
                var groupBy = this.VisitExpressionList(select.GroupBy);
                var skip = this.Visit(select.Skip);
                var take = this.Visit(select.Take);
                var columns = this.VisitColumnDeclarations(select.Columns);

                if (this.currentFrom != select.From || where != select.Where || orderBy != select.OrderBy || groupBy != select.GroupBy || take != select.Take || skip != select.Skip || columns != select.Columns)
                {
                    return new DbSelectExpression(select.Alias, columns, this.currentFrom, where, orderBy, groupBy, select.IsDistinct, skip, take, select.IsReverse);
                }

                return select;
            }
            finally
            {
                this.currentFrom = saveCurrentFrom;
            }
        }

        protected override Expression VisitMemberAccess(MemberExpression m)
        {
            var source = this.Visit(m.Expression);

            if (source is DbEntityExpression ex && this.mapping.IsRelationship(ex.Entity, m.Member))
            {
                var projection = this.Visit(this.mapper.GetMemberExpression(source, ex.Entity, m.Member)) as DbProjectionExpression;

                if (this.currentFrom != null && this.mapping.IsSingletonRelationship(ex.Entity, m.Member))
                {
                    projection = this.language.AddOuterJoinTest(projection);

                    this.currentFrom = new DbJoinExpression(JoinType.OuterApply, this.currentFrom, projection.Select, null);

                    return projection.Projector;
                }

                return projection;
            }
            else
            {
                var result = DbQueryBinder.BindMember(source, m.Member);

                if (result is MemberExpression mex && mex.Member == m.Member && mex.Expression == m.Expression)
                {
                    return m;
                }

                return result;
            }
        }
    }
}