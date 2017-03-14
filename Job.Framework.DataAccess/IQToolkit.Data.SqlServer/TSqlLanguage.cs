using IQToolkit.Data.Common;
using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;

namespace IQToolkit.Data.SqlServer
{
    internal class TSqlLanguage : QueryLanguage
    {
        private static readonly char[] splitChars = new char[] { '.' };

        private static TSqlLanguage langer;

        public static TSqlLanguage Default
        {
            get
            {
                if (langer == null)
                {
                    Interlocked.CompareExchange(ref langer, new TSqlLanguage(), null);
                }

                return langer;
            }
        }

        public override QueryTypeSystem TypeSystem { get; }

        public override bool AllowsMultipleCommands
        {
            get { return true; }
        }

        public override bool AllowSubqueryInSelectWithoutFrom
        {
            get { return true; }
        }

        public override bool AllowDistinctInAggregates
        {
            get { return true; }
        }

        public TSqlLanguage()
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
            return new DbFunctionExpression(TypeHelper.GetMemberType(member), "SCOPE_IDENTITY()", null);
        }

        public override QueryLinguist CreateLinguist(QueryTranslator translator)
        {
            return new TSqlLinguist(this, translator);
        }

        private class TSqlLinguist : QueryLinguist
        {
            public TSqlLinguist(TSqlLanguage language, QueryTranslator translator) : base(language, translator)
            {

            }

            public override Expression Translate(Expression expression)
            {
                expression = DbOrderByRewriter.Rewrite(this.Language, expression);
                expression = base.Translate(expression);
                expression = DbSkipToRowNumberRewriter.Rewrite(this.Language, expression);
                expression = DbOrderByRewriter.Rewrite(this.Language, expression);

                return expression;
            }

            public override string Format(Expression expression)
            {
                return TSqlFormatter.Format(expression, this.Language);
            }
        }
    }
}