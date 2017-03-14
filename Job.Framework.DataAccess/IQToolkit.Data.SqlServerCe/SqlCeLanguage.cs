using IQToolkit.Data.Common;
using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;

namespace IQToolkit.Data.SqlServerCe
{
    internal class SqlCeLanguage : QueryLanguage
    {
        private static readonly char[] splitChars = new char[] { '.' };

        private static SqlCeLanguage langer;

        public static SqlCeLanguage Default
        {
            get
            {
                if (langer == null)
                {
                    Interlocked.CompareExchange(ref langer, new SqlCeLanguage(), null);
                }

                return langer;
            }
        }

        public override QueryTypeSystem TypeSystem { get; }

        public override bool AllowsMultipleCommands
        {
            get { return false; }
        }

        public override bool AllowDistinctInAggregates
        {
            get { return false; }
        }


        public SqlCeLanguage()
        {
            this.TypeSystem = new DbTypeSystem();
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
            return new DbFunctionExpression(TypeHelper.GetMemberType(member), "@@IDENTITY", null);
        }

        public override QueryLinguist CreateLinguist(QueryTranslator translator)
        {
            return new SqlCeLinguist(this, translator);
        }

        private class SqlCeLinguist : QueryLinguist
        {
            public SqlCeLinguist(SqlCeLanguage language, QueryTranslator translator) : base(language, translator)
            {

            }

            public override Expression Translate(Expression expression)
            {
                expression = DbOrderByRewriter.Rewrite(this.Language, expression);
                expression = base.Translate(expression);
                expression = DbSkipToNestedOrderByRewriter.Rewrite(this.Language, expression);
                expression = DbOrderByRewriter.Rewrite(this.Language, expression);
                expression = DbUnusedColumnRemover.Remove(expression);
                expression = DbRedundantSubqueryRemover.Remove(expression);
                expression = DbScalarSubqueryRewriter.Rewrite(this.Language, expression);

                return expression;
            }

            public override string Format(Expression expression)
            {
                return SqlCeFormatter.Format(expression, this.Language);
            }
        }
    }
}