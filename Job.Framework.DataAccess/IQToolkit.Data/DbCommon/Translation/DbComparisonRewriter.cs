using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace IQToolkit.Data.Common
{
    internal class DbComparisonRewriter : DbExpressionVisitor
    {
        private readonly QueryMapping mapping;

        private DbComparisonRewriter(QueryMapping mapping)
        {
            this.mapping = mapping;
        }

        public static Expression Rewrite(QueryMapping mapping, Expression expression)
        {
            return new DbComparisonRewriter(mapping).Visit(expression);
        }

        protected override Expression VisitBinary(BinaryExpression b)
        {
            switch (b.NodeType)
            {
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                    {
                        var result = this.Compare(b);

                        if (result == b)
                        {
                            goto default;
                        }

                        return this.Visit(result);
                    }
                default:
                    {
                        return base.VisitBinary(b);
                    }
            }
        }

        protected Expression SkipConvert(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Convert)
            {
                expression = ((UnaryExpression)expression).Operand;
            }

            return expression;
        }

        protected Expression Compare(BinaryExpression bop)
        {
            var e1 = this.SkipConvert(bop.Left);
            var e2 = this.SkipConvert(bop.Right);

            var oj1 = e1 as DbOuterJoinedExpression;
            var oj2 = e2 as DbOuterJoinedExpression;

            var entity1 = oj1 != null ? oj1.Expression as DbEntityExpression : e1 as DbEntityExpression;
            var entity2 = oj2 != null ? oj2.Expression as DbEntityExpression : e2 as DbEntityExpression;

            var negate = bop.NodeType == ExpressionType.NotEqual;

            if (oj1 != null && e2.NodeType == ExpressionType.Constant && (e2 as ConstantExpression).Value == null)
            {
                return MakeIsNull(oj1.Test, negate);
            }
            else if (oj2 != null && e1.NodeType == ExpressionType.Constant && (e1 as ConstantExpression).Value == null)
            {
                return MakeIsNull(oj2.Test, negate);
            }

            if (entity1 != null)
            {
                return this.MakePredicate(e1, e2, this.mapping.GetPrimaryKeyMembers(entity1.Entity), negate);
            }
            else if (entity2 != null)
            {
                return this.MakePredicate(e1, e2, this.mapping.GetPrimaryKeyMembers(entity2.Entity), negate);
            }

            var dm1 = this.GetDefinedMembers(e1);
            var dm2 = this.GetDefinedMembers(e2);

            if (dm1 == null && dm2 == null)
            {
                return bop;
            }

            if (dm1 != null && dm2 != null)
            {
                var names1 = new HashSet<string>(dm1.Select(m => m.Name));
                var names2 = new HashSet<string>(dm2.Select(m => m.Name));

                if (names1.IsSubsetOf(names2) && names2.IsSubsetOf(names1))
                {
                    return MakePredicate(e1, e2, dm1, negate);
                }
            }
            else if (dm1 != null)
            {
                return MakePredicate(e1, e2, dm1, negate);
            }
            else if (dm2 != null)
            {
                return MakePredicate(e1, e2, dm2, negate);
            }

            throw new InvalidOperationException("Cannot compare two constructed types with different sets of members assigned.");
        }

        protected Expression MakeIsNull(Expression expression, bool negate)
        {
            var isnull = new DbIsNullExpression(expression) as Expression;

            if (negate == false)
            {
                return isnull;
            }

            return Expression.Not(isnull);
        }

        protected Expression MakePredicate(Expression e1, Expression e2, IEnumerable<MemberInfo> members, bool negate)
        {
            var pred = members.Select(m => DbQueryBinder.BindMember(e1, m).Equal(DbQueryBinder.BindMember(e2, m))).Join(ExpressionType.And);

            if (negate)
            {
                pred = Expression.Not(pred);
            }

            return pred;
        }

        private IEnumerable<MemberInfo> GetDefinedMembers(Expression expr)
        {
            if (expr is MemberInitExpression mini)
            {
                var members = mini.Bindings.Select(b => FixMember(b.Member));

                if (mini.NewExpression.Members != null)
                {
                    members.Concat(mini.NewExpression.Members.Select(m => FixMember(m)));
                }

                return members;
            }

            if (expr is NewExpression nex && nex.Members != null)
            {
                return nex.Members.Select(m => FixMember(m));
            }

            return null;
        }

        private static MemberInfo FixMember(MemberInfo member)
        {
            if (member.MemberType == MemberTypes.Method && member.Name.StartsWith("get_"))
            {
                return member.DeclaringType.GetProperty(member.Name.Substring(4));
            }

            return member;
        }
    }
}