using IQToolkit.Data.Common;
using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;

namespace IQToolkit.Data.Access
{
    internal class AccessLanguage : QueryLanguage
    {
        private static AccessLanguage langer;

        public static AccessLanguage Default
        {
            get
            {
                if (langer == null)
                {
                    Interlocked.CompareExchange(ref langer, new AccessLanguage(), null);
                }

                return langer;
            }
        }

        public override QueryTypeSystem TypeSystem { get; }


        public AccessLanguage()
        {
            this.TypeSystem = new AccessTypeSystem();
        }

        public override string Quote(string name)
        {
            if (name.StartsWith("[") && name.EndsWith("]"))
            {
                return name;
            }
            else
            {
                return "[" + name + "]";
            }
        }

        public override Expression GetGeneratedIdExpression(MemberInfo member)
        {
            return new DbFunctionExpression(TypeHelper.GetMemberType(member), "@@IDENTITY", null);
        }

        public override QueryLinguist CreateLinguist(QueryTranslator translator)
        {
            return new AccessLinguist(this, translator);
        }

        private class AccessLinguist : QueryLinguist
        {
            public AccessLinguist(AccessLanguage language, QueryTranslator translator) : base(language, translator)
            {

            }

            public override Expression Translate(Expression expression)
            {
                expression = DbOrderByRewriter.Rewrite(this.Language, expression);
                expression = base.Translate(expression);
                expression = DbCrossJoinIsolator.Isolate(expression);
                expression = DbSkipToNestedOrderByRewriter.Rewrite(this.Language, expression);
                expression = DbOrderByRewriter.Rewrite(this.Language, expression);
                expression = DbUnusedColumnRemover.Remove(expression);
                expression = DbRedundantSubqueryRemover.Remove(expression);

                return expression;
            }

            public override string Format(Expression expression)
            {
                return AccessFormatter.Format(expression);
            }
        }

        private class AccessTypeSystem : DbTypeSystem
        {
            public override int StringDefaultSize
            {
                get { return 2000; }
            }

            public override int BinaryDefaultSize
            {
                get { return 4000; }
            }

            public override QueryType GetQueryType(string typeName, string[] args, bool isNotNull)
            {
                if (string.Compare(typeName, "Memo", StringComparison.OrdinalIgnoreCase) == 0)
                {
                    return base.GetQueryType("varchar", new[] { "max" }, isNotNull);
                }

                return base.GetQueryType(typeName, args, isNotNull);
            }

            public override SqlDbType GetSqlType(string typeName)
            {
                if (string.Compare(typeName, "Memo", true) == 0)
                {
                    return SqlDbType.VarChar;
                }
                else if (string.Compare(typeName, "Currency", true) == 0)
                {
                    return SqlDbType.Decimal;
                }
                else if (string.Compare(typeName, "ReplicationID", true) == 0)
                {
                    return SqlDbType.UniqueIdentifier;
                }
                else if (string.Compare(typeName, "YesNo", true) == 0)
                {
                    return SqlDbType.Bit;
                }
                else if (string.Compare(typeName, "LongInteger", true) == 0)
                {
                    return SqlDbType.BigInt;
                }
                else if (string.Compare(typeName, "VarWChar", true) == 0)
                {
                    return SqlDbType.NVarChar;
                }
                else
                {
                    return base.GetSqlType(typeName);
                }
            }

            public override string GetVariableDeclaration(QueryType type, bool suppressSize)
            {
                var sb = new StringBuilder();
                var sqlType = (DbQueryType)type;
                var sqlDbType = sqlType.SqlDbType;

                switch (sqlDbType)
                {
                    case SqlDbType.BigInt:
                    case SqlDbType.Bit:
                    case SqlDbType.DateTime:
                    case SqlDbType.Int:
                    case SqlDbType.Money:
                    case SqlDbType.SmallDateTime:
                    case SqlDbType.SmallInt:
                    case SqlDbType.SmallMoney:
                    case SqlDbType.Timestamp:
                    case SqlDbType.TinyInt:
                    case SqlDbType.UniqueIdentifier:
                    case SqlDbType.Variant:
                    case SqlDbType.Xml:
                        {
                            sb.Append(sqlDbType);

                            break;
                        }
                    case SqlDbType.Binary:
                    case SqlDbType.Char:
                    case SqlDbType.NChar:
                        {
                            sb.Append(sqlDbType);

                            if (type.Length > 0 && !suppressSize)
                            {
                                sb.Append("(");
                                sb.Append(type.Length);
                                sb.Append(")");
                            }

                            break;
                        }
                    case SqlDbType.Image:
                    case SqlDbType.NText:
                    case SqlDbType.NVarChar:
                    case SqlDbType.Text:
                    case SqlDbType.VarBinary:
                    case SqlDbType.VarChar:
                        {
                            sb.Append(sqlDbType);

                            if (type.Length > 0 && !suppressSize)
                            {
                                sb.Append("(");
                                sb.Append(type.Length);
                                sb.Append(")");
                            }

                            break;
                        }
                    case SqlDbType.Decimal:
                        {
                            sb.Append("Currency");

                            break;
                        }
                    case SqlDbType.Float:
                    case SqlDbType.Real:
                        {
                            sb.Append(sqlDbType);

                            if (type.Precision != 0)
                            {
                                sb.Append("(");
                                sb.Append(type.Precision);

                                if (type.Scale != 0)
                                {
                                    sb.Append(",");
                                    sb.Append(type.Scale);
                                }

                                sb.Append(")");
                            }

                            break;
                        }
                }

                return sb.ToString();
            }
        }
    }
}