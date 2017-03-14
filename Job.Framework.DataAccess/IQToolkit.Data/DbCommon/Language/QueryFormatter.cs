using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace IQToolkit.Data.Common
{
    internal class SqlFormatter : DbExpressionVisitor
    {
        protected enum Indentation
        {
            Same,
            Inner,
            Outer
        }

        private int depth;
        private int indent = 2;
        private readonly StringBuilder sb;
        private readonly Dictionary<TableAlias, string> aliases;

        protected bool IsNested { get; private set; }

        protected bool ForDebug { get; private set; }

        protected bool HideColumnAliases { get; private set; }

        protected bool HideTableAliases { get; private set; }

        protected virtual QueryLanguage Language { get; private set; }

        protected SqlFormatter(QueryLanguage language, bool forDebug = false)
        {
            this.Language = language;
            this.sb = new StringBuilder();
            this.aliases = new Dictionary<TableAlias, string>();
            this.ForDebug = forDebug;
        }

        public static string Format(Expression expression, bool forDebug = false)
        {
            var formatter = new SqlFormatter(null, forDebug);

            if (formatter != null)
            {
                formatter.Visit(expression);
            }

            return formatter.ToString();
        }

        protected void Write(object value)
        {
            this.sb.Append(value);
        }

        protected virtual void WriteParameterName(string name)
        {
            this.Write("@" + name);
        }

        protected virtual void WriteVariableName(string name)
        {
            this.WriteParameterName(name);
        }

        protected virtual void WriteAsAliasName(string aliasName)
        {
            this.Write("AS ");
            this.WriteAliasName(aliasName);
        }

        protected virtual void WriteAliasName(string aliasName)
        {
            this.Write(aliasName);
        }

        protected virtual void WriteAsColumnName(string columnName)
        {
            this.Write("AS ");
            this.WriteColumnName(columnName);
        }

        protected virtual void WriteColumnName(string columnName)
        {
            this.Write(this.Language != null ? this.Language.Quote(columnName) : columnName);
        }

        protected virtual void WriteTableName(string tableName)
        {
            this.Write(this.Language != null ? this.Language.Quote(tableName) : tableName);
        }

        protected void WriteLine(Indentation style)
        {
            sb.AppendLine();

            this.Indent(style);

            for (int i = 0, n = this.depth * this.indent; i < n; i++)
            {
                this.Write(" ");
            }
        }

        protected void Indent(Indentation style)
        {
            if (style == Indentation.Inner)
            {
                this.depth++;
            }

            if (style == Indentation.Outer)
            {
                this.depth--;
            }
        }

        protected virtual string GetAliasName(TableAlias alias)
        {
            if (this.aliases.TryGetValue(alias, out string name) == false)
            {
                this.aliases.Add(alias, (name = "A" + alias.GetHashCode() + "?"));
            }

            return name;
        }

        protected void AddAlias(TableAlias alias)
        {
            if (this.aliases.TryGetValue(alias, out string name) == false)
            {
                this.aliases.Add(alias, (name = "t" + this.aliases.Count));
            }
        }

        protected virtual void AddAliases(Expression expr)
        {
            if (expr is DbAliasedExpression ax)
            {
                this.AddAlias(ax.Alias);
            }
            else if (expr is DbJoinExpression jx)
            {
                this.AddAliases(jx.Left);
                this.AddAliases(jx.Right);
            }
        }

        protected override Expression Visit(Expression exp)
        {
            if (exp == null)
            {
                return null;
            }

            switch (exp.NodeType)
            {
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                case ExpressionType.Not:
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                case ExpressionType.UnaryPlus:
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.Coalesce:
                case ExpressionType.RightShift:
                case ExpressionType.LeftShift:
                case ExpressionType.ExclusiveOr:
                case ExpressionType.Power:
                case ExpressionType.Conditional:
                case ExpressionType.Constant:
                case ExpressionType.MemberAccess:
                case ExpressionType.Call:
                case ExpressionType.New:
                case (ExpressionType)DbExpressionType.Table:
                case (ExpressionType)DbExpressionType.Column:
                case (ExpressionType)DbExpressionType.Select:
                case (ExpressionType)DbExpressionType.Join:
                case (ExpressionType)DbExpressionType.Aggregate:
                case (ExpressionType)DbExpressionType.Scalar:
                case (ExpressionType)DbExpressionType.Exists:
                case (ExpressionType)DbExpressionType.In:
                case (ExpressionType)DbExpressionType.AggregateSubquery:
                case (ExpressionType)DbExpressionType.IsNull:
                case (ExpressionType)DbExpressionType.Between:
                case (ExpressionType)DbExpressionType.RowCount:
                case (ExpressionType)DbExpressionType.Projection:
                case (ExpressionType)DbExpressionType.NamedValue:
                case (ExpressionType)DbExpressionType.Insert:
                case (ExpressionType)DbExpressionType.Update:
                case (ExpressionType)DbExpressionType.Delete:
                case (ExpressionType)DbExpressionType.Block:
                case (ExpressionType)DbExpressionType.If:
                case (ExpressionType)DbExpressionType.Declaration:
                case (ExpressionType)DbExpressionType.Variable:
                case (ExpressionType)DbExpressionType.Function:
                    {
                        return base.Visit(exp);
                    }
                case ExpressionType.ArrayLength:
                case ExpressionType.Quote:
                case ExpressionType.TypeAs:
                case ExpressionType.ArrayIndex:
                case ExpressionType.TypeIs:
                case ExpressionType.Parameter:
                case ExpressionType.Lambda:
                case ExpressionType.NewArrayInit:
                case ExpressionType.NewArrayBounds:
                case ExpressionType.Invoke:
                case ExpressionType.MemberInit:
                case ExpressionType.ListInit:
                default:
                    {
                        if (this.ForDebug)
                        {
                            this.Write(string.Format("?{0}?(", exp.NodeType));
                            base.Visit(exp);
                            this.Write(")");

                            return exp;
                        }

                        throw new NotSupportedException(string.Format("The LINQ expression node of type {0} is not supported", exp.NodeType));
                    }
            }
        }

        protected override Expression VisitMemberAccess(MemberExpression m)
        {
            if (this.ForDebug)
            {
                this.Visit(m.Expression);
                this.Write(".");
                this.Write(m.Member.Name);

                return m;
            }

            throw new NotSupportedException(string.Format("The member access '{0}' is not supported", m.Member));
        }

        protected override Expression VisitMethodCall(MethodCallExpression m)
        {
            if (m.Method.DeclaringType == typeof(Decimal))
            {
                switch (m.Method.Name)
                {
                    case "Add":
                    case "Subtract":
                    case "Multiply":
                    case "Divide":
                    case "Ceiling":
                    case "Remainder":
                        {
                            this.Write("(");
                            this.VisitValue(m.Arguments[0]);
                            this.Write(" ");
                            this.Write(GetOperator(m.Method.Name));
                            this.Write(" ");
                            this.VisitValue(m.Arguments[1]);
                            this.Write(")");

                            return m;
                        }
                    case "Negate":
                        {
                            this.Write("-");
                            this.Visit(m.Arguments[0]);
                            this.Write("");

                            return m;
                        }
                    case "Compare":
                        {
                            this.Visit(Expression.Condition
                            (
                                Expression.Equal(m.Arguments[0], m.Arguments[1]),
                                Expression.Constant(0),
                                Expression.Condition
                                (
                                    Expression.LessThan(m.Arguments[0], m.Arguments[1]),
                                    Expression.Constant(-1),
                                    Expression.Constant(1)
                                )
                             ));

                            return m;
                        }
                }
            }
            else if (m.Method.Name == "ToString" && m.Object.Type == typeof(string))
            {
                return this.Visit(m.Object);
            }
            else if (m.Method.Name == "Equals")
            {
                if (m.Method.IsStatic && m.Method.DeclaringType == typeof(object))
                {
                    this.Write("(");
                    this.Visit(m.Arguments[0]);
                    this.Write(" = ");
                    this.Visit(m.Arguments[1]);
                    this.Write(")");

                    return m;
                }
                else if (!m.Method.IsStatic && m.Arguments.Count == 1 && m.Arguments[0].Type == m.Object.Type)
                {
                    this.Write("(");
                    this.Visit(m.Object);
                    this.Write(" = ");
                    this.Visit(m.Arguments[0]);
                    this.Write(")");

                    return m;
                }
            }

            if (this.ForDebug)
            {
                if (m.Object != null)
                {
                    this.Visit(m.Object);
                    this.Write(".");
                }

                this.Write($"?{ m.Method.Name }?");
                this.Write("(");

                for (var i = 0; i < m.Arguments.Count; i++)
                {
                    if (i > 0)
                    {
                        this.Write(", ");
                    }

                    this.Visit(m.Arguments[i]);
                }

                this.Write(")");

                return m;
            }

            throw new NotSupportedException($"The method '{ m.Method.Name }' is not supported");
        }

        protected virtual bool IsInteger(Type type)
        {
            return TypeHelper.IsInteger(type);
        }

        protected override NewExpression VisitNew(NewExpression nex)
        {
            if (this.ForDebug)
            {
                this.Write("?new?");
                this.Write(nex.Type.Name);
                this.Write("(");

                for (var i = 0; i < nex.Arguments.Count; i++)
                {
                    if (i > 0)
                    {
                        this.Write(", ");
                    }

                    this.Visit(nex.Arguments[i]);
                }

                this.Write(")");

                return nex;
            }
            else
            {
                throw new NotSupportedException($"The constructor for '{ nex.Constructor.DeclaringType }' is not supported");
            }
        }

        protected override Expression VisitUnary(UnaryExpression u)
        {
            var op = this.GetOperator(u);

            switch (u.NodeType)
            {
                case ExpressionType.Not:
                    {
                        if (u.Operand is DbIsNullExpression)
                        {
                            this.Visit((u.Operand as DbIsNullExpression).Expression);
                            this.Write(" IS NOT NULL");
                        }
                        else if (IsBoolean(u.Operand.Type) || op.Length > 1)
                        {
                            this.Write(op);
                            this.Write(" ");
                            this.VisitPredicate(u.Operand);
                        }
                        else
                        {
                            this.Write(op);
                            this.VisitValue(u.Operand);
                        }

                        break;
                    }
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                    {
                        this.Write(op);
                        this.VisitValue(u.Operand);

                        break;
                    }
                case ExpressionType.UnaryPlus:
                    {
                        this.VisitValue(u.Operand);

                        break;
                    }
                case ExpressionType.Convert:
                    {
                        this.Visit(u.Operand);

                        break;
                    }
                default:
                    {
                        if (this.ForDebug)
                        {
                            this.Write($"?{ u.NodeType }?");
                            this.Write("(");
                            this.Visit(u.Operand);
                            this.Write(")");

                            return u;
                        }

                        throw new NotSupportedException($"The unary operator '{ u.NodeType }' is not supported");
                    }
            }

            return u;
        }

        protected override Expression VisitBinary(BinaryExpression b)
        {
            var op = this.GetOperator(b);
            var left = b.Left;
            var right = b.Right;

            this.Write("(");

            switch (b.NodeType)
            {
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                    {
                        if (this.IsBoolean(left.Type))
                        {
                            this.VisitPredicate(left);
                            this.Write(" ");
                            this.Write(op);
                            this.Write(" ");
                            this.VisitPredicate(right);
                        }
                        else
                        {
                            this.VisitValue(left);
                            this.Write(" ");
                            this.Write(op);
                            this.Write(" ");
                            this.VisitValue(right);
                        }

                        break;
                    }
                case ExpressionType.Equal:
                    {
                        if (right.NodeType == ExpressionType.Constant)
                        {
                            var ce = right as ConstantExpression;

                            if (ce.Value == null)
                            {
                                this.Visit(left);
                                this.Write(" IS NULL");

                                break;
                            }
                        }
                        else if (left.NodeType == ExpressionType.Constant)
                        {
                            var ce = left as ConstantExpression;

                            if (ce.Value == null)
                            {
                                this.Visit(right);
                                this.Write(" IS NULL");

                                break;
                            }
                        }

                        goto case ExpressionType.LessThan;
                    }
                case ExpressionType.NotEqual:
                    {
                        if (right.NodeType == ExpressionType.Constant)
                        {
                            var ce = right as ConstantExpression;

                            if (ce.Value == null)
                            {
                                this.Visit(left);
                                this.Write(" IS NOT NULL");

                                break;
                            }
                        }
                        else if (left.NodeType == ExpressionType.Constant)
                        {
                            var ce = left as ConstantExpression;

                            if (ce.Value == null)
                            {
                                this.Visit(right);
                                this.Write(" IS NOT NULL");

                                break;
                            }
                        }

                        goto case ExpressionType.LessThan;
                    }
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                    {
                        if (left.NodeType == ExpressionType.Call && right.NodeType == ExpressionType.Constant)
                        {
                            var mc = left as MethodCallExpression;
                            var ce = right as ConstantExpression;

                            if (ce.Value != null && ce.Value.GetType() == typeof(int) && ((int)ce.Value) == 0)
                            {
                                if (mc.Method.Name == "CompareTo" && !mc.Method.IsStatic && mc.Arguments.Count == 1)
                                {
                                    left = mc.Object;
                                    right = mc.Arguments[0];
                                }
                                else if ((mc.Method.DeclaringType == typeof(string) || mc.Method.DeclaringType == typeof(decimal)) && mc.Method.Name == "Compare" && mc.Method.IsStatic && mc.Arguments.Count == 2)
                                {
                                    left = mc.Arguments[0];
                                    right = mc.Arguments[1];
                                }
                            }
                        }

                        goto case ExpressionType.Add;
                    }
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                case ExpressionType.ExclusiveOr:
                case ExpressionType.LeftShift:
                case ExpressionType.RightShift:
                    {
                        this.VisitValue(left);
                        this.Write(" ");
                        this.Write(op);
                        this.Write(" ");
                        this.VisitValue(right);

                        break;
                    }
                default:
                    {
                        if (this.ForDebug)
                        {
                            this.Write($"?{ b.NodeType }?");
                            this.Write("(");
                            this.Visit(b.Left);
                            this.Write(", ");
                            this.Visit(b.Right);
                            this.Write(")");

                            return b;
                        }

                        throw new NotSupportedException($"The binary operator '{ b.NodeType }' is not supported");
                    }
            }

            this.Write(")");

            return b;
        }

        protected virtual string GetOperator(string methodName)
        {
            switch (methodName)
            {
                case "Add": return "+";
                case "Subtract": return "-";
                case "Multiply": return "*";
                case "Divide": return "/";
                case "Negate": return "-";
                case "Remainder": return "%";

                default: return null;
            }
        }

        protected virtual string GetOperator(UnaryExpression u)
        {
            switch (u.NodeType)
            {
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked: return "-";
                case ExpressionType.UnaryPlus: return "+";
                case ExpressionType.Not: return IsBoolean(u.Operand.Type) ? "NOT" : "~";
                default:
                    {
                        return string.Empty;
                    }
            }
        }

        protected virtual string GetOperator(BinaryExpression b)
        {
            switch (b.NodeType)
            {
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                    return IsBoolean(b.Left.Type) ? "AND" : "&";
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                    return IsBoolean(b.Left.Type) ? "OR" : "|";
                case ExpressionType.Equal:
                    return "=";
                case ExpressionType.NotEqual:
                    return "<>";
                case ExpressionType.LessThan:
                    return "<";
                case ExpressionType.LessThanOrEqual:
                    return "<=";
                case ExpressionType.GreaterThan:
                    return ">";
                case ExpressionType.GreaterThanOrEqual:
                    return ">=";
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                    return "+";
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                    return "-";
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                    return "*";
                case ExpressionType.Divide:
                    return "/";
                case ExpressionType.Modulo:
                    return "%";
                case ExpressionType.ExclusiveOr:
                    return "^";
                case ExpressionType.LeftShift:
                    return "<<";
                case ExpressionType.RightShift:
                    return ">>";
                default:
                    {
                        return string.Empty;
                    }
            }
        }

        protected virtual bool IsBoolean(Type type)
        {
            return type == typeof(bool) || type == typeof(bool?);
        }

        protected virtual bool IsPredicate(Expression expr)
        {
            switch (expr.NodeType)
            {
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                    return IsBoolean((expr as BinaryExpression).Type);
                case ExpressionType.Not:
                    return IsBoolean((expr as UnaryExpression).Type);
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case (ExpressionType)DbExpressionType.IsNull:
                case (ExpressionType)DbExpressionType.Between:
                case (ExpressionType)DbExpressionType.Exists:
                case (ExpressionType)DbExpressionType.In:
                    return true;
                case ExpressionType.Call:
                    return IsBoolean((expr as MethodCallExpression).Type);
                default:
                    {
                        return false;
                    }
            }
        }

        protected virtual Expression VisitPredicate(Expression expr)
        {
            this.Visit(expr);

            if (IsPredicate(expr) == false)
            {
                this.Write(" <> 0");
            }

            return expr;
        }

        protected virtual Expression VisitValue(Expression expr)
        {
            return this.Visit(expr);
        }

        protected override Expression VisitConditional(ConditionalExpression c)
        {
            if (this.ForDebug)
            {
                this.Write("?iff?(");
                this.Visit(c.Test);
                this.Write(", ");
                this.Visit(c.IfTrue);
                this.Write(", ");
                this.Visit(c.IfFalse);
                this.Write(")");

                return c;
            }

            throw new NotSupportedException("Conditional expressions not supported");
        }

        protected override Expression VisitConstant(ConstantExpression c)
        {
            this.WriteValue(c.Value);

            return c;
        }

        protected virtual void WriteValue(object value)
        {
            if (value == null)
            {
                this.Write("NULL"); return;
            }

            var type = value.GetType();
            var typeInfo = type.GetTypeInfo();

            if (typeInfo.IsEnum)
            {
                this.Write(Convert.ChangeType(value, Enum.GetUnderlyingType(type))); return;
            }

            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Boolean:
                    {
                        this.Write(((bool)value) ? 1 : 0);

                        break;
                    }
                case TypeCode.String:
                    {
                        this.Write("'");
                        this.Write(value);
                        this.Write("'");

                        break;
                    }
                case TypeCode.Single:
                case TypeCode.Double:
                    {
                        var str = value.ToString();

                        if (str.Contains('.') == false)
                        {
                            str += ".0";
                        }

                        this.Write(str);

                        break;
                    }
                case TypeCode.Object:
                    {
                        throw new NotSupportedException($"The constant for '{ value }' is not supported");
                    }
                default:
                    {
                        this.Write(value);

                        break;
                    }
            }
        }

        protected override Expression VisitColumn(DbColumnExpression column)
        {
            if (column.Alias != null && !this.HideColumnAliases)
            {
                this.WriteAliasName(GetAliasName(column.Alias));
                this.Write(".");
            }

            this.WriteColumnName(column.Name);

            return column;
        }

        protected override Expression VisitProjection(DbProjectionExpression proj)
        {
            if ((proj.Projector is DbColumnExpression) || this.ForDebug)
            {
                this.Write("(");
                this.WriteLine(Indentation.Inner);
                this.Visit(proj.Select);
                this.Write(")");
                this.Indent(Indentation.Outer);
            }
            else
            {
                throw new NotSupportedException("Non-scalar projections cannot be translated to SQL.");
            }

            return proj;
        }

        protected override Expression VisitSelect(DbSelectExpression select)
        {
            this.AddAliases(select.From);

            this.Write("SELECT ");

            if (select.IsDistinct)
            {
                this.Write("DISTINCT ");
            }

            if (select.Take != null)
            {
                this.WriteTopClause(select.Take);
            }

            this.WriteColumns(select.Columns);

            if (select.From != null)
            {
                this.WriteLine(Indentation.Same);
                this.Write("FROM ");
                this.VisitSource(select.From);
            }

            if (select.Where != null)
            {
                this.WriteLine(Indentation.Same);
                this.Write("WHERE ");
                this.VisitPredicate(select.Where);
            }

            if (select.GroupBy != null && select.GroupBy.Count > 0)
            {
                this.WriteLine(Indentation.Same);
                this.Write("GROUP BY ");

                for (int i = 0, n = select.GroupBy.Count; i < n; i++)
                {
                    if (i > 0)
                    {
                        this.Write(", ");
                    }

                    this.VisitValue(select.GroupBy[i]);
                }
            }

            if (select.OrderBy != null && select.OrderBy.Count > 0)
            {
                this.WriteLine(Indentation.Same);
                this.Write("ORDER BY ");

                for (int i = 0, n = select.OrderBy.Count; i < n; i++)
                {
                    var exp = select.OrderBy[i];

                    if (i > 0)
                    {
                        this.Write(", ");
                    }

                    this.VisitValue(exp.Expression);

                    if (exp.OrderType != OrderType.Ascending)
                    {
                        this.Write(" DESC");
                    }
                }
            }

            return select;
        }

        protected virtual void WriteTopClause(Expression expression)
        {
            this.Write("TOP (");
            this.Visit(expression);
            this.Write(") ");
        }

        protected virtual void WriteColumns(ReadOnlyCollection<DbColumnDeclaration> columns)
        {
            if (columns.Count > 0)
            {
                for (int i = 0, n = columns.Count; i < n; i++)
                {
                    var column = columns[i];

                    if (i > 0)
                    {
                        this.Write(", ");
                    }

                    var c = this.VisitValue(column.Expression) as DbColumnExpression;

                    if (string.IsNullOrEmpty(column.Name) == false && (c == null || c.Name != column.Name))
                    {
                        this.Write(" ");
                        this.WriteAsColumnName(column.Name);
                    }
                }
            }
            else
            {
                this.Write("NULL ");

                if (this.IsNested)
                {
                    this.WriteAsColumnName("tmp");
                    this.Write(" ");
                }
            }
        }

        protected override Expression VisitSource(Expression source)
        {
            var saveIsNested = this.IsNested;

            this.IsNested = true;

            switch ((DbExpressionType)source.NodeType)
            {
                case DbExpressionType.Table:
                    {
                        var table = source as DbTableExpression;

                        this.WriteTableName(table.Name);

                        if (this.HideTableAliases == false)
                        {
                            this.Write(" ");
                            this.WriteAsAliasName(GetAliasName(table.Alias));
                        }

                        break;
                    }
                case DbExpressionType.Select:
                    {
                        var select = source as DbSelectExpression;

                        this.Write("(");
                        this.WriteLine(Indentation.Inner);
                        this.Visit(select);
                        this.WriteLine(Indentation.Same);
                        this.Write(") ");
                        this.WriteAsAliasName(GetAliasName(select.Alias));
                        this.Indent(Indentation.Outer);

                        break;
                    }
                case DbExpressionType.Join:
                    {
                        this.VisitJoin(source as DbJoinExpression);

                        break;
                    }
                default:
                    {
                        throw new InvalidOperationException("Select source is not valid type");
                    }
            }

            this.IsNested = saveIsNested;

            return source;
        }

        protected override Expression VisitJoin(DbJoinExpression join)
        {
            this.VisitJoinLeft(join.Left);

            this.WriteLine(Indentation.Same);

            switch (join.JoinType)
            {
                case JoinType.CrossJoin:
                    {
                        this.Write("CROSS JOIN ");

                        break;
                    }
                case JoinType.InnerJoin:
                    {
                        this.Write("INNER JOIN ");

                        break;
                    }
                case JoinType.CrossApply:
                    {
                        this.Write("CROSS APPLY ");

                        break;
                    }
                case JoinType.OuterApply:
                    {
                        this.Write("OUTER APPLY ");

                        break;
                    }
                case JoinType.LeftOuter:
                case JoinType.SingletonLeftOuter:
                    {
                        this.Write("LEFT OUTER JOIN ");

                        break;
                    }
            }

            this.VisitJoinRight(join.Right);

            if (join.Condition != null)
            {
                this.WriteLine(Indentation.Inner);
                this.Write("ON ");
                this.VisitPredicate(join.Condition);
                this.Indent(Indentation.Outer);
            }

            return join;
        }

        protected virtual Expression VisitJoinLeft(Expression source)
        {
            return this.VisitSource(source);
        }

        protected virtual Expression VisitJoinRight(Expression source)
        {
            return this.VisitSource(source);
        }

        protected virtual void WriteAggregateName(string aggregateName)
        {
            switch (aggregateName)
            {
                case "Average": this.Write("AVG"); break;
                case "LongCount": this.Write("COUNT"); break;
                default: this.Write(aggregateName.ToUpper()); break;
            }
        }

        protected virtual bool RequiresAsteriskWhenNoArgument(string aggregateName)
        {
            return aggregateName == "Count" || aggregateName == "LongCount";
        }

        protected override Expression VisitAggregate(DbAggregateExpression aggregate)
        {
            this.WriteAggregateName(aggregate.AggregateName);
            this.Write("(");

            if (aggregate.IsDistinct)
            {
                this.Write("DISTINCT ");
            }

            if (aggregate.Argument != null)
            {
                this.VisitValue(aggregate.Argument);
            }
            else if (RequiresAsteriskWhenNoArgument(aggregate.AggregateName))
            {
                this.Write("*");
            }

            this.Write(")");

            return aggregate;
        }

        protected override Expression VisitIsNull(DbIsNullExpression isnull)
        {
            this.VisitValue(isnull.Expression);
            this.Write(" IS NULL");

            return isnull;
        }

        protected override Expression VisitBetween(DbBetweenExpression between)
        {
            this.VisitValue(between.Expression);
            this.Write(" BETWEEN ");
            this.VisitValue(between.Lower);
            this.Write(" AND ");
            this.VisitValue(between.Upper);

            return between;
        }

        protected override Expression VisitRowNumber(DbRowNumberExpression rowNumber)
        {
            throw new NotSupportedException();
        }

        protected override Expression VisitScalar(DbScalarExpression subquery)
        {
            this.Write("(");
            this.WriteLine(Indentation.Inner);
            this.Visit(subquery.Select);
            this.WriteLine(Indentation.Same);
            this.Write(")");
            this.Indent(Indentation.Outer);

            return subquery;
        }

        protected override Expression VisitExists(DbExistsExpression exists)
        {
            this.Write("EXISTS(");
            this.WriteLine(Indentation.Inner);
            this.Visit(exists.Select);
            this.WriteLine(Indentation.Same);
            this.Write(")");
            this.Indent(Indentation.Outer);

            return exists;
        }

        protected override Expression VisitIn(DbInExpression inExp)
        {
            if (inExp.Values != null)
            {
                if (inExp.Values.Count == 0)
                {
                    this.Write("0 <> 0");
                }
                else
                {
                    this.VisitValue(inExp.Expression);
                    this.Write(" IN (");

                    for (int i = 0, n = inExp.Values.Count; i < n; i++)
                    {
                        if (i > 0)
                        {
                            this.Write(", ");
                        }

                        this.VisitValue(inExp.Values[i]);
                    }

                    this.Write(")");
                }
            }
            else
            {
                this.VisitValue(inExp.Expression);
                this.Write(" IN (");
                this.WriteLine(Indentation.Inner);
                this.Visit(inExp.Select);
                this.WriteLine(Indentation.Same);
                this.Write(")");
                this.Indent(Indentation.Outer);
            }

            return inExp;
        }

        protected override Expression VisitNamedValue(DbNamedValueExpression value)
        {
            this.WriteParameterName(value.Name);

            return value;
        }

        protected override Expression VisitInsert(DbInsertCommand insert)
        {
            this.Write("INSERT INTO ");
            this.WriteTableName(insert.Table.Name);
            this.Write("(");

            for (int i = 0, n = insert.Assignments.Count; i < n; i++)
            {
                var ca = insert.Assignments[i];

                if (i > 0)
                {
                    this.Write(", ");
                }

                this.WriteColumnName(ca.Column.Name);
            }

            this.Write(")");
            this.WriteLine(Indentation.Same);
            this.Write("VALUES (");

            for (int i = 0, n = insert.Assignments.Count; i < n; i++)
            {
                var ca = insert.Assignments[i];

                if (i > 0)
                {
                    this.Write(", ");
                }

                this.Visit(ca.Expression);
            }

            this.Write(")");

            return insert;
        }

        protected override Expression VisitUpdate(DbUpdateCommand update)
        {
            this.Write("UPDATE ");
            this.WriteTableName(update.Table.Name);
            this.WriteLine(Indentation.Same);

            var saveHide = this.HideColumnAliases;

            this.HideColumnAliases = true;
            this.Write("SET ");

            for (int i = 0, n = update.Assignments.Count; i < n; i++)
            {
                var ca = update.Assignments[i];

                if (i > 0)
                {
                    this.Write(", ");
                }

                this.Visit(ca.Column);
                this.Write(" = ");
                this.Visit(ca.Expression);
            }

            if (update.Where != null)
            {
                this.WriteLine(Indentation.Same);
                this.Write("WHERE ");
                this.VisitPredicate(update.Where);
            }

            this.HideColumnAliases = saveHide;

            return update;
        }

        protected override Expression VisitDelete(DbDeleteCommand delete)
        {
            this.Write("DELETE FROM ");

            var saveHideTable = this.HideTableAliases;
            var saveHideColumn = this.HideColumnAliases;

            this.HideTableAliases = true;
            this.HideColumnAliases = true;

            this.VisitSource(delete.Table);

            if (delete.Where != null)
            {
                this.WriteLine(Indentation.Same);
                this.Write("WHERE ");
                this.VisitPredicate(delete.Where);
            }

            this.HideTableAliases = saveHideTable;
            this.HideColumnAliases = saveHideColumn;

            return delete;
        }

        protected override Expression VisitIf(DbIFCommand ifx)
        {
            throw new NotSupportedException();
        }

        protected override Expression VisitBlock(DbBlockCommand block)
        {
            throw new NotSupportedException();
        }

        protected override Expression VisitDeclaration(DbDeclarationCommand decl)
        {
            throw new NotSupportedException();
        }

        protected override Expression VisitVariable(DbVariableExpression vex)
        {
            this.WriteVariableName(vex.Name);

            return vex;
        }

        protected virtual void VisitStatement(Expression expression)
        {
            if (expression is DbProjectionExpression p)
            {
                this.Visit(p.Select);
            }
            else
            {
                this.Visit(expression);
            }
        }

        protected override Expression VisitFunction(DbFunctionExpression func)
        {
            this.Write(func.Name);

            if (func.Arguments.Count > 0)
            {
                this.Write("(");

                for (int i = 0, n = func.Arguments.Count; i < n; i++)
                {
                    if (i > 0)
                    {
                        this.Write(", ");
                    }

                    this.Visit(func.Arguments[i]);
                }

                this.Write(")");
            }

            return func;
        }

        public override string ToString()
        {
            return this.sb.ToString();
        }
    }
}