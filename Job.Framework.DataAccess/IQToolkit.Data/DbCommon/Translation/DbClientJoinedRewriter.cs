using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace IQToolkit.Data.Common
{
    internal class DbClientJoinedRewriter : DbExpressionVisitor
    {
        private readonly QueryPolicy policy;
        private readonly QueryLanguage language;
        private bool isTopLevel = true;
        private DbSelectExpression currentSelect;
        private MemberInfo currentMember;
        private bool canJoinOnClient = true;

        private DbClientJoinedRewriter(QueryPolicy policy, QueryLanguage language)
        {
            this.policy = policy;
            this.language = language;
        }

        public static Expression Rewrite(QueryPolicy policy, QueryLanguage language, Expression expression)
        {
            return new DbClientJoinedRewriter(policy, language).Visit(expression);
        }

        protected override MemberAssignment VisitMemberAssignment(MemberAssignment assignment)
        {
            var saveMember = this.currentMember;

            this.currentMember = assignment.Member;

            var e = this.Visit(assignment.Expression);

            this.currentMember = saveMember;

            return this.UpdateMemberAssignment(assignment, assignment.Member, e);
        }

        protected override Expression VisitMemberAndExpression(MemberInfo member, Expression expression)
        {
            var saveMember = this.currentMember;

            this.currentMember = member;

            var e = this.Visit(expression);

            this.currentMember = saveMember;

            return e;
        }

        protected override Expression VisitProjection(DbProjectionExpression proj)
        {
            var save = this.currentSelect;

            this.currentSelect = proj.Select;

            try
            {
                if (this.isTopLevel == false)
                {
                    if (this.CanJoinOnClient(this.currentSelect))
                    {
                        var newOuterSelect = DbQueryDuplicator.Duplicate(save) as DbSelectExpression;

                        var newInnerSelect = DbColumnMapper.Map(proj.Select, newOuterSelect.Alias, save.Alias) as DbSelectExpression;
                        var newInnerProjection = this.language.AddOuterJoinTest(new DbProjectionExpression(newInnerSelect, proj.Projector));

                        if (newInnerProjection != null)
                        {
                            newInnerSelect = newInnerProjection.Select;
                        }

                        var newProjector = newInnerProjection.Projector;

                        var newAlias = new TableAlias();
                        var pc = DbColumnProjector.ProjectColumns(this.language, newProjector, null, newAlias, newOuterSelect.Alias, newInnerSelect.Alias);

                        var join = new DbJoinExpression(JoinType.OuterApply, newOuterSelect, newInnerSelect, null);
                        var joinedSelect = new DbSelectExpression(newAlias, pc.Columns, join, null, null, null, proj.IsSingleton, null, null, false);

                        this.currentSelect = joinedSelect;

                        if (pc != null)
                        {
                            newProjector = this.Visit(pc.Projector);
                        }

                        var outerKeys = new List<Expression>();
                        var innerKeys = new List<Expression>();

                        if (this.GetEquiJoinKeyExpressions(newInnerSelect.Where, newOuterSelect.Alias, outerKeys, innerKeys))
                        {
                            var outerKey = outerKeys.Select(k => DbColumnMapper.Map(k, save.Alias, newOuterSelect.Alias));
                            var innerKey = innerKeys.Select(k => DbColumnMapper.Map(k, joinedSelect.Alias, ((DbColumnExpression)k).Alias));
                            var newProjection = new DbProjectionExpression(joinedSelect, newProjector, proj.Aggregator);

                            return new DbClientJoinExpression(newProjection, outerKey, innerKey);
                        }
                    }
                    else
                    {
                        var saveJoin = this.canJoinOnClient;


                        this.canJoinOnClient = false;

                        var result = base.VisitProjection(proj);

                        this.canJoinOnClient = saveJoin;

                        return result;
                    }
                }
                else
                {
                    this.isTopLevel = false;
                }

                return base.VisitProjection(proj);
            }
            finally
            {
                this.currentSelect = save;
            }
        }

        private bool CanJoinOnClient(DbSelectExpression select)
        {
            return
            (
                this.canJoinOnClient
                && this.currentMember != null
                && !this.policy.IsDeferLoaded(this.currentMember)
                && !select.IsDistinct
                && (select.GroupBy == null || select.GroupBy.Count == 0)
                && !DbAggregateChecker.HasAggregates(select)
            );
        }

        private bool GetEquiJoinKeyExpressions(Expression predicate, TableAlias outerAlias, List<Expression> outerExpressions, List<Expression> innerExpressions)
        {
            if (predicate.NodeType == ExpressionType.Equal)
            {
                var b = predicate as BinaryExpression;
                var leftCol = this.GetColumnExpression(b.Left);
                var rightCol = this.GetColumnExpression(b.Right);

                if (leftCol != null && rightCol != null)
                {
                    if (leftCol.Alias == outerAlias)
                    {
                        outerExpressions.Add(b.Left);
                        innerExpressions.Add(b.Right);

                        return true;
                    }
                    else if (rightCol.Alias == outerAlias)
                    {
                        innerExpressions.Add(b.Left);
                        outerExpressions.Add(b.Right);

                        return true;
                    }
                }
            }

            var hadKey = false;
            var parts = predicate.Split(ExpressionType.And, ExpressionType.AndAlso);

            if (parts.Length > 1)
            {
                foreach (var part in parts)
                {
                    var hasOuterAliasReference = DbReferencedAliasGatherer.Gather(part).Contains(outerAlias);

                    if (hasOuterAliasReference)
                    {
                        if (GetEquiJoinKeyExpressions(part, outerAlias, outerExpressions, innerExpressions) == false)
                        {
                            return false;
                        }

                        hadKey = true;
                    }
                }
            }

            return hadKey;
        }

        private DbColumnExpression GetColumnExpression(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Convert || expression.NodeType == ExpressionType.ConvertChecked)
            {
                expression = (expression as UnaryExpression).Operand;
            }

            return expression as DbColumnExpression;
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