using System.Collections.Generic;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbCrossApplyRewriter : DbExpressionVisitor
    {
        private readonly QueryLanguage language;

        private DbCrossApplyRewriter(QueryLanguage language)
        {
            this.language = language;
        }

        public static Expression Rewrite(QueryLanguage language, Expression expression)
        {
            return new DbCrossApplyRewriter(language).Visit(expression);
        }

        protected override Expression VisitJoin(DbJoinExpression join)
        {
            join = base.VisitJoin(join) as DbJoinExpression;

            if (join.JoinType == JoinType.CrossApply || join.JoinType == JoinType.OuterApply)
            {
                if (join.Right is DbTableExpression)
                {
                    return new DbJoinExpression(JoinType.CrossJoin, join.Left, join.Right, null);
                }

                if (join.Right is DbSelectExpression select && select.Take == null && select.Skip == null && !DbAggregateChecker.HasAggregates(select) && (select.GroupBy == null || select.GroupBy.Count == 0))
                {
                    var selectWithoutWhere = select.SetWhere(null);
                    var referencedAliases = DbReferencedAliasGatherer.Gather(selectWithoutWhere);
                    var declaredAliases = DbDeclaredAliasGatherer.Gather(join.Left);

                    if (referencedAliases != null)
                    {
                        referencedAliases.IntersectWith(declaredAliases);
                    }

                    if (referencedAliases.Count == 0)
                    {
                        var where = select.Where;

                        select = selectWithoutWhere;

                        var pc = DbColumnProjector.ProjectColumns(this.language, where, select.Columns, select.Alias, DbDeclaredAliasGatherer.Gather(select.From));

                        select = select.SetColumns(pc.Columns);

                        where = pc.Projector;

                        var jt = (where == null) ? JoinType.CrossJoin : (join.JoinType == JoinType.CrossApply ? JoinType.InnerJoin : JoinType.LeftOuter);

                        return new DbJoinExpression(jt, join.Left, select, where);
                    }
                }
            }

            return join;
        }

        private bool CanBeColumn(Expression expr)
        {
            return expr != null && expr.NodeType == (ExpressionType)DbExpressionType.Column;
        }
    }
}