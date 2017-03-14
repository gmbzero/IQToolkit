using IQToolkit.Data.Common;
using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;

namespace IQToolkit.Data.SQLite
{
    internal sealed class SQLiteLanguage : QueryLanguage
    {
        private static readonly char[] splitChars = new char[] { '.' };

        private static SQLiteLanguage langer;

        public static SQLiteLanguage Default
        {
            get
            {
                if (langer == null)
                {
                    Interlocked.CompareExchange(ref langer, new SQLiteLanguage(), null);
                }

                return langer;
            }
        }

        public override QueryTypeSystem TypeSystem { get; }

        public SQLiteLanguage()
        {
            this.TypeSystem = new SQLiteTypeSystem();
        }

        public override string Quote(string name)
        {
            if (name.StartsWith("[") && name.EndsWith("]"))
            {
                return name;
            }
            else if (name.IndexOf('.') > 0)
            {
                return "[" + string.Join("].[", name.Split(splitChars, StringSplitOptions.RemoveEmptyEntries)) + "]";
            }
            else
            {
                return "[" + name + "]";
            }
        }

        public override Expression GetGeneratedIdExpression(MemberInfo member)
        {
            return new DbFunctionExpression(TypeHelper.GetMemberType(member), "last_insert_rowid()", null);
        }

        public override Expression GetRowsAffectedExpression(Expression command)
        {
            return new DbFunctionExpression(typeof(int), "changes()", null);
        }

        public override bool IsRowsAffectedExpressions(Expression expression)
        {
            var fex = expression as DbFunctionExpression;

            if (fex == null)
            {
                return false;
            }

            return fex.Name == "changes()";
        }

        public override QueryLinguist CreateLinguist(QueryTranslator translator)
        {
            return new SQLiteLinguist(this, translator);
        }

        private class SQLiteLinguist : QueryLinguist
        {
            public SQLiteLinguist(SQLiteLanguage language, QueryTranslator translator) : base(language, translator)
            {

            }

            public override Expression Translate(Expression expression)
            {
                expression = DbOrderByRewriter.Rewrite(this.Language, expression);
                expression = base.Translate(expression);
                expression = DbUnusedColumnRemover.Remove(expression);

                return expression;
            }

            public override string Format(Expression expression)
            {
                return SQLiteFormatter.Format(expression);
            }
        }

        private class SQLiteTypeSystem : DbTypeSystem
        {
            public override SqlDbType GetSqlType(string typeName)
            {
                if (string.Compare(typeName, "TEXT", true) == 0 || string.Compare(typeName, "CHAR", true) == 0 || string.Compare(typeName, "CLOB", true) == 0 || string.Compare(typeName, "VARYINGCHARACTER", true) == 0 || string.Compare(typeName, "NATIONALVARYINGCHARACTER", true) == 0)
                {
                    return SqlDbType.VarChar;
                }
                else if (string.Compare(typeName, "INT", true) == 0 || string.Compare(typeName, "INTEGER", true) == 0)
                {
                    return SqlDbType.BigInt;
                }
                else if (string.Compare(typeName, "BLOB", true) == 0)
                {
                    return SqlDbType.Binary;
                }
                else if (string.Compare(typeName, "BOOLEAN", true) == 0)
                {
                    return SqlDbType.Bit;
                }
                else if (string.Compare(typeName, "NUMERIC", true) == 0)
                {
                    return SqlDbType.Decimal;
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
                    case SqlDbType.SmallInt:
                    case SqlDbType.Int:
                    case SqlDbType.TinyInt:
                        {
                            sb.Append("INTEGER");

                            break;
                        }
                    case SqlDbType.Bit:
                        {
                            sb.Append("BOOLEAN");

                            break;
                        }
                    case SqlDbType.SmallDateTime:
                        {
                            sb.Append("DATETIME");

                            break;
                        }
                    case SqlDbType.Char:
                    case SqlDbType.NChar:
                        {
                            sb.Append("CHAR");

                            if (type.Length > 0 && !suppressSize)
                            {
                                sb.Append("(");
                                sb.Append(type.Length);
                                sb.Append(")");
                            }

                            break;
                        }
                    case SqlDbType.Variant:
                    case SqlDbType.Binary:
                    case SqlDbType.Image:
                    case SqlDbType.UniqueIdentifier:
                        {
                            sb.Append("BLOB");

                            if (type.Length > 0 && !suppressSize)
                            {
                                sb.Append("(");
                                sb.Append(type.Length);
                                sb.Append(")");
                            }

                            break;
                        }
                    case SqlDbType.Xml:
                    case SqlDbType.NText:
                    case SqlDbType.NVarChar:
                    case SqlDbType.Text:
                    case SqlDbType.VarBinary:
                    case SqlDbType.VarChar:
                        {
                            sb.Append("TEXT");

                            if (type.Length > 0 && !suppressSize)
                            {
                                sb.Append("(");
                                sb.Append(type.Length);
                                sb.Append(")");
                            }

                            break;
                        }
                    case SqlDbType.Decimal:
                    case SqlDbType.Money:
                    case SqlDbType.SmallMoney:
                        {
                            sb.Append("NUMERIC");

                            if (type.Precision != 0)
                            {
                                sb.Append("(");
                                sb.Append(type.Precision);
                                sb.Append(")");
                            }

                            break;
                        }
                    case SqlDbType.Float:
                    case SqlDbType.Real:
                        {
                            sb.Append("FLOAT");

                            if (type.Precision != 0)
                            {
                                sb.Append("(");
                                sb.Append(type.Precision);
                                sb.Append(")");
                            }

                            break;
                        }
                    case SqlDbType.Date:
                    case SqlDbType.DateTime:
                    case SqlDbType.Timestamp:
                    default:
                        {
                            sb.Append(sqlDbType);

                            break;
                        }
                }

                return sb.ToString();
            }
        }
    }
}