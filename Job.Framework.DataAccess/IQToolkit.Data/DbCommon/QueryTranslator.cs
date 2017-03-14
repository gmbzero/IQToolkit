using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class QueryTranslator
    {
        public QueryLinguist Linguist { get; }

        public QueryMapper Mapper { get; }

        public QueryPolice Police { get; }

        public QueryTranslator(QueryLanguage language, QueryMapping mapping, QueryPolicy policy)
        {
            this.Linguist = language.CreateLinguist(this);
            this.Mapper = mapping.CreateMapper(this);
            this.Police = policy.CreatePolice(this);
        }

        public virtual Expression Translate(Expression expression)
        {
            expression = ExpressionEvaluator.Eval(expression, this.Mapper.Mapping.CanBeEvaluatedLocally);

            expression = this.Mapper.Translate(expression);

            expression = this.Police.Translate(expression);

            expression = this.Linguist.Translate(expression);

            return expression;
        }
    }
}