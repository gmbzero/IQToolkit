using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace IQToolkit.Data.Common
{
    internal class QueryLinguist
    {
        public QueryLanguage Language { get; }

        public QueryTranslator Translator { get; }

        public QueryLinguist(QueryLanguage language, QueryTranslator translator)
        {
            this.Language = language;
            this.Translator = translator;
        }

        public virtual Expression Translate(Expression expression)
        {
            if (expression != null)
            {
                expression = DbUnusedColumnRemover.Remove(expression);
                expression = DbRedundantColumnRemover.Remove(expression);
                expression = DbRedundantSubqueryRemover.Remove(expression);
            }

            var rewritten = DbCrossApplyRewriter.Rewrite(this.Language, expression);

            if (rewritten != null)
            {
                rewritten = DbCrossJoinRewriter.Rewrite(rewritten);
            }

            if (rewritten != expression)
            {
                expression = rewritten;
                expression = DbUnusedColumnRemover.Remove(expression);
                expression = DbRedundantSubqueryRemover.Remove(expression);
                expression = DbRedundantJoinRemover.Remove(expression);
                expression = DbRedundantColumnRemover.Remove(expression);
            }

            return expression;
        }

        public virtual string Format(Expression expression)
        {
            return SqlFormatter.Format(expression);
        }

        public virtual Expression Parameterize(Expression expression)
        {
            return DbParameterizer.Parameterize(this.Language, expression);
        }
    }
}