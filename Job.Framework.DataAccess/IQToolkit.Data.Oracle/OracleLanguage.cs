﻿using IQToolkit.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;

namespace IQToolkit.Data.Oracle
{
    internal class OracleLanguage : QueryLanguage
    {
        private static OracleLanguage langer;

        public static OracleLanguage Default
        {
            get
            {
                if (langer == null)
                {
                    Interlocked.CompareExchange(ref langer, new OracleLanguage(), null);
                }

                return langer;
            }
        }

        public override QueryTypeSystem TypeSystem { get; }

        public OracleLanguage()
        {
            this.TypeSystem = new DbTypeSystem();
        }

        public override bool AllowsMultipleCommands
        {
            get { return false; }
        }

        public override bool AllowDistinctInAggregates
        {
            get { return true; }
        }

        public override string Quote(string name)
        {
            return "`" + name + "`";
        }

        private static readonly char[] splitChars = new char[] { '.' };

        public override Expression GetGeneratedIdExpression(MemberInfo member)
        {
            return new DbFunctionExpression(TypeHelper.GetMemberType(member), "LAST_INSERT_ID()", null);
        }

        public override Expression GetRowsAffectedExpression(Expression command)
        {
            return new DbFunctionExpression(typeof(int), "ROW_COUNT()", null);
        }

        public override bool IsRowsAffectedExpressions(Expression expression)
        {
            var fex = expression as DbFunctionExpression;

            if (fex == null)
            {
                return false;
            }

            return fex.Name == "ROW_COUNT()";
        }

        public override QueryLinguist CreateLinguist(QueryTranslator translator)
        {
            return new OracleLinguist(this, translator);
        }

        private class OracleLinguist : QueryLinguist
        {
            public OracleLinguist(OracleLanguage language, QueryTranslator translator) : base(language, translator)
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
                return OracleFormatter.Format(expression, this.Language);
            }
        }
    }
}