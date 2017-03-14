using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbCrossJoinRewriter : DbExpressionVisitor
    {
        private Expression currentWhere;

        public static Expression Rewrite(Expression expression)
        {
            return new DbCrossJoinRewriter().Visit(expression);
        }

        protected override Expression VisitSelect(DbSelectExpression select)
        {
            var saveWhere = this.currentWhere;

            try
            {
                this.currentWhere = select.Where;

                var result =  base.VisitSelect(select) as DbSelectExpression;

                if (this.currentWhere != result.Where)
                {
                    return result.SetWhere(this.currentWhere);
                }

                return result;
            }
            finally
            {
                this.currentWhere = saveWhere;
            }
        }

        protected override Expression VisitJoin(DbJoinExpression join)
        {
            join = base.VisitJoin(join) as DbJoinExpression;

            if (join.JoinType == JoinType.CrossJoin && this.currentWhere != null)
            {
                var declaredLeft = DbDeclaredAliasGatherer.Gather(join.Left);
                var declaredRight = DbDeclaredAliasGatherer.Gather(join.Right);
                var declared = new HashSet<TableAlias>(declaredLeft.Union(declaredRight));
                var exprs = this.currentWhere.Split(ExpressionType.And, ExpressionType.AndAlso);
                var good = exprs.Where(e => CanBeJoinCondition(e, declaredLeft, declaredRight, declared)).ToList();

                if (good.Count > 0)
                {
                    var condition = good.Join(ExpressionType.And);

                    join = this.UpdateJoin(join, JoinType.InnerJoin, join.Left, join.Right, condition);

                    var newWhere = exprs.Where(e => !good.Contains(e)).Join(ExpressionType.And);

                    this.currentWhere = newWhere;
                }
            }

            return join;
        }

        private bool CanBeJoinCondition(Expression expression, HashSet<TableAlias> left, HashSet<TableAlias> right, HashSet<TableAlias> all)
        {
            var referenced = DbReferencedAliasGatherer.Gather(expression);
            var leftOkay = referenced.Intersect(left).Any();
            var rightOkay = referenced.Intersect(right).Any();
            var subset = referenced.IsSubsetOf(all);

            return leftOkay && rightOkay && subset;
        }
    }
}