using System.Linq.Expressions;
using System.Reflection;
using System.Threading;

namespace IQToolkit.Data.Common
{
    internal class QueryPolicy
    {
        public static QueryPolicy queryPolicy;

        public static QueryPolicy Default
        {
            get
            {
                if (queryPolicy == null)
                {
                    Interlocked.CompareExchange(ref queryPolicy, new QueryPolicy(), null);
                }

                return queryPolicy;
            }
        }

        public virtual bool IsIncluded(MemberInfo member)
        {
            return false;
        }

        public virtual bool IsDeferLoaded(MemberInfo member)
        {
            return false;
        }

        public virtual QueryPolice CreatePolice(QueryTranslator translator)
        {
            return new QueryPolice(this, translator);
        }
    }

    internal class QueryPolice
    {
        public QueryPolicy Policy { get; }

        public QueryTranslator Translator { get; }

        public QueryPolice(QueryPolicy policy, QueryTranslator translator)
        {
            this.Policy = policy;
            this.Translator = translator;
        }

        public virtual Expression ApplyPolicy(Expression expression, MemberInfo member)
        {
            return expression;
        }

        public virtual Expression Translate(Expression expression)
        {
            var rewritten = DbRelationshipIncluder.Include(this.Translator.Mapper, expression);

            if (rewritten != expression)
            {
                expression = rewritten;
                expression = DbUnusedColumnRemover.Remove(expression);
                expression = DbRedundantColumnRemover.Remove(expression);
                expression = DbRedundantSubqueryRemover.Remove(expression);
                expression = DbRedundantJoinRemover.Remove(expression);
            }

            rewritten = DbSingletonProjectionRewriter.Rewrite(this.Translator.Linguist.Language, expression);

            if (rewritten != expression)
            {
                expression = rewritten;
                expression = DbUnusedColumnRemover.Remove(expression);
                expression = DbRedundantColumnRemover.Remove(expression);
                expression = DbRedundantSubqueryRemover.Remove(expression);
                expression = DbRedundantJoinRemover.Remove(expression);
            }

            rewritten = DbClientJoinedRewriter.Rewrite(this.Policy, this.Translator.Linguist.Language, expression);

            if (rewritten != expression)
            {
                expression = rewritten;
                expression = DbUnusedColumnRemover.Remove(expression);
                expression = DbRedundantColumnRemover.Remove(expression);
                expression = DbRedundantSubqueryRemover.Remove(expression);
                expression = DbRedundantJoinRemover.Remove(expression);
            }

            return expression;
        }

        public virtual Expression BuildExecutionPlan(Expression query, Expression provider)
        {
            return DbExecutionBuilder.Build(this.Translator.Linguist, this.Policy, query, provider);
        }
    }
}