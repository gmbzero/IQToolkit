using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbParameterizer : DbExpressionVisitor
    {
        private int iParam = 0;
        private readonly QueryLanguage language;
        private readonly Dictionary<TypeAndValue, DbNamedValueExpression> map;
        private readonly Dictionary<HashedExpression, DbNamedValueExpression> pmap;

        private DbParameterizer(QueryLanguage language)
        {
            this.language = language;
            this.map = new Dictionary<TypeAndValue, DbNamedValueExpression>();
            this.pmap = new Dictionary<HashedExpression, DbNamedValueExpression>();
        }

        public static Expression Parameterize(QueryLanguage language, Expression expression)
        {
            return new DbParameterizer(language).Visit(expression);
        }

        protected override Expression VisitProjection(DbProjectionExpression proj)
        {
            return this.UpdateProjection(proj, this.Visit(proj.Select) as DbSelectExpression, proj.Projector, proj.Aggregator);
        }

        protected override Expression VisitUnary(UnaryExpression u)
        {
            if (u.NodeType == ExpressionType.Convert && u.Operand.NodeType == ExpressionType.ArrayIndex)
            {
                var b = u.Operand as BinaryExpression;

                if (IsConstantOrParameter(b.Left) && IsConstantOrParameter(b.Right))
                {
                    return this.GetNamedValue(u);
                }
            }

            return base.VisitUnary(u);
        }

        private static bool IsConstantOrParameter(Expression e)
        {
            return e != null && e.NodeType == ExpressionType.Constant || e.NodeType == ExpressionType.Parameter;
        }

        protected override Expression VisitBinary(BinaryExpression b)
        {
            var left = this.Visit(b.Left);
            var right = this.Visit(b.Right);

            if (left.NodeType == (ExpressionType)DbExpressionType.NamedValue && right.NodeType == (ExpressionType)DbExpressionType.Column)
            {
                var nv = left as DbNamedValueExpression;
                var c = right as DbColumnExpression;

                left = new DbNamedValueExpression(nv.Name, c.QueryType, nv.Value);
            }
            else if (right.NodeType == (ExpressionType)DbExpressionType.NamedValue && left.NodeType == (ExpressionType)DbExpressionType.Column)
            {
                var nv = right as DbNamedValueExpression;
                var c = left as DbColumnExpression;

                right = new DbNamedValueExpression(nv.Name, c.QueryType, nv.Value);
            }

            return this.UpdateBinary(b, left, right, b.Conversion, b.IsLiftedToNull, b.Method);
        }

        protected override DbColumnAssignment VisitColumnAssignment(DbColumnAssignment ca)
        {
            ca = base.VisitColumnAssignment(ca);

            var expression = ca.Expression;

            if (expression is DbNamedValueExpression nv)
            {
                expression = new DbNamedValueExpression(nv.Name, ca.Column.QueryType, nv.Value);
            }

            return this.UpdateColumnAssignment(ca, ca.Column, expression);
        }

        protected override Expression VisitConstant(ConstantExpression c)
        {
            if (c.Value != null && !IsNumeric(c.Value.GetType()))
            {
                var tv = new TypeAndValue(c.Type, c.Value);

                if (this.map.TryGetValue(tv, out DbNamedValueExpression nv) == false)
                {
                    var name = "p" + (iParam++);

                    nv = new DbNamedValueExpression(name, this.language.TypeSystem.GetColumnType(c.Type), c);

                    this.map.Add(tv, nv);
                }

                return nv;
            }

            return c;
        }

        protected override Expression VisitParameter(ParameterExpression p)
        {
            return this.GetNamedValue(p);
        }

        protected override Expression VisitMemberAccess(MemberExpression m)
        {
            m = base.VisitMemberAccess(m) as MemberExpression;

            if (m.Expression is DbNamedValueExpression nv)
            {
                return GetNamedValue(Expression.MakeMemberAccess(nv.Value, m.Member));
            }

            return m;
        }

        private Expression GetNamedValue(Expression e)
        {
            var he = new HashedExpression(e);

            if (this.pmap.TryGetValue(he, out DbNamedValueExpression nv) == false)
            {
                var name = "p" + (iParam++);

                nv = new DbNamedValueExpression(name, this.language.TypeSystem.GetColumnType(e.Type), e);

                this.pmap.Add(he, nv);
            }

            return nv;
        }

        private bool IsNumeric(Type type)
        {
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Boolean:
                case TypeCode.Byte:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.SByte:
                case TypeCode.Single:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64: return true;
                default:
                    {
                        return false;
                    }
            }
        }

        private struct TypeAndValue : IEquatable<TypeAndValue>
        {
            private readonly Type type;
            private readonly object value;
            private readonly int hash;

            public TypeAndValue(Type type, object value)
            {
                this.type = type;
                this.value = value;
                this.hash = type.GetHashCode() + (value != null ? value.GetHashCode() : 0);
            }

            public override bool Equals(object obj)
            {
                if ((obj is TypeAndValue) == false)
                {

                    return false;
                }

                return this.Equals((TypeAndValue)obj);
            }

            public bool Equals(TypeAndValue vt)
            {
                return vt.type == this.type && object.Equals(vt.value, this.value);
            }

            public override int GetHashCode()
            {
                return this.hash;
            }
        }

        private struct HashedExpression : IEquatable<HashedExpression>
        {
            private readonly Expression expression;
            private readonly int hashCode;

            public HashedExpression(Expression expression)
            {
                this.expression = expression;
                this.hashCode = Hasher.ComputeHash(expression);
            }

            public override bool Equals(object obj)
            {
                if ((obj is HashedExpression) == false)
                {
                    return false;
                }

                return this.Equals((HashedExpression)obj);
            }

            public bool Equals(HashedExpression other)
            {
                return this.hashCode == other.hashCode && DbExpressionComparer.AreEqual(this.expression, other.expression);
            }

            public override int GetHashCode()
            {
                return this.hashCode;
            }

            private class Hasher : DbExpressionVisitor
            {
                private int hc;

                public static int ComputeHash(Expression expression)
                {
                    var hasher = new Hasher();

                    if (hasher != null)
                    {
                        hasher.Visit(expression);
                    }

                    return hasher.hc;
                }

                protected override Expression VisitConstant(ConstantExpression c)
                {
                    hc = hc + ((c.Value != null) ? c.Value.GetHashCode() : 0);

                    return c;
                }
            }
        }
    }
}