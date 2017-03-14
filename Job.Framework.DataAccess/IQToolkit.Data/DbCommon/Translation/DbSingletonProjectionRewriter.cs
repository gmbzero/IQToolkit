using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbSingletonProjectionRewriter : DbExpressionVisitor
    {
        private bool isTopLevel = true;
        private DbSelectExpression currentSelect;
        private readonly QueryLanguage language;

        private DbSingletonProjectionRewriter(QueryLanguage language)
        {
            this.language = language;
        }

        public static Expression Rewrite(QueryLanguage language, Expression expression)
        {
            return new DbSingletonProjectionRewriter(language).Visit(expression);
        }

        protected override Expression VisitClientJoin(DbClientJoinExpression join)
        {
            var saveTop = this.isTopLevel;
            var saveSelect = this.currentSelect;

            this.isTopLevel = true;
            this.currentSelect = null;

            var result = base.VisitClientJoin(join);

            this.isTopLevel = saveTop;
            this.currentSelect = saveSelect;

            return result;
        }

        protected override Expression VisitProjection(DbProjectionExpression proj)
        {
            if (isTopLevel)
            {
                isTopLevel = false;

                this.currentSelect = proj.Select;

                var projector = this.Visit(proj.Projector);

                if (projector != proj.Projector || this.currentSelect != proj.Select)
                {
                    return new DbProjectionExpression(this.currentSelect, projector, proj.Aggregator);
                }

                return proj;
            }

            if (proj.IsSingleton && this.CanJoinOnServer(this.currentSelect))
            {
                var newAlias = new TableAlias();

                this.currentSelect = this.currentSelect.AddRedundantSelect(this.language, newAlias);

                var source = DbColumnMapper.Map(proj.Select, newAlias, this.currentSelect.Alias) as DbSelectExpression;

                var pex = this.language.AddOuterJoinTest(new DbProjectionExpression(source, proj.Projector));

                var pc = DbColumnProjector.ProjectColumns(this.language, pex.Projector, this.currentSelect.Columns, this.currentSelect.Alias, newAlias, proj.Select.Alias);

                var join = new DbJoinExpression(JoinType.OuterApply, this.currentSelect.From, pex.Select, null);

                this.currentSelect = new DbSelectExpression(this.currentSelect.Alias, pc.Columns, join, null);

                return this.Visit(pc.Projector);
            }

            var saveTop = this.isTopLevel;
            var saveSelect = this.currentSelect;

            this.isTopLevel = true;
            this.currentSelect = null;

            var result = base.VisitProjection(proj);

            this.isTopLevel = saveTop;
            this.currentSelect = saveSelect;

            return result;
        }

        private bool CanJoinOnServer(DbSelectExpression select)
        {
            return select.IsDistinct == false && (select.GroupBy == null || select.GroupBy.Count == 0) && !DbAggregateChecker.HasAggregates(select);
        }

        protected override Expression VisitSubquery(SubqueryExpression subquery)
        {
            return subquery;
        }

        protected override Expression VisitCommand(DbCommandExpression command)
        {
            this.isTopLevel = true;

            return base.VisitCommand(command);
        }
    }
}