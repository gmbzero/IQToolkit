using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace IQToolkit
{
    internal static class ExpressionEvaluator
    {
        public static Expression Eval(Expression expression)
        {
            return Eval(expression, null, null);
        }

        public static Expression Eval(Expression expression, Func<Expression, bool> fnCanBeEvaluated)
        {
            return Eval(expression, fnCanBeEvaluated, null);
        }

        public static Expression Eval(Expression expression, Func<Expression, bool> fnCanBeEvaluated, Func<ConstantExpression, Expression> fnPostEval)
        {
            if (fnCanBeEvaluated == null)
            {
                fnCanBeEvaluated = ExpressionEvaluator.CanBeEvaluatedLocally;
            }

            return SubtreeEvaluator.Eval(Nominator.Nominate(fnCanBeEvaluated, expression), fnPostEval, expression);
        }

        private static bool CanBeEvaluatedLocally(Expression expression)
        {
            return expression.NodeType != ExpressionType.Parameter;
        }

        private class SubtreeEvaluator : ExpressionVisitor
        {
            private readonly HashSet<Expression> candidates;
            private readonly Func<ConstantExpression, Expression> onEval;

            private SubtreeEvaluator(HashSet<Expression> candidates, Func<ConstantExpression, Expression> onEval)
            {
                this.candidates = candidates;
                this.onEval = onEval;
            }

            public static Expression Eval(HashSet<Expression> candidates, Func<ConstantExpression, Expression> onEval, Expression exp)
            {
                return new SubtreeEvaluator(candidates, onEval).Visit(exp);
            }

            protected override Expression Visit(Expression exp)
            {
                if (exp == null)
                {
                    return null;
                }

                if (this.candidates.Contains(exp))
                {
                    return this.Evaluate(exp);
                }

                return base.Visit(exp);
            }

            private Expression PostEval(ConstantExpression e)
            {
                if (this.onEval != null)
                {
                    return this.onEval(e);
                }

                return e;
            }

            private Expression Evaluate(Expression e)
            {
                var type = e.Type;

                if (e.NodeType == ExpressionType.Convert)
                {
                    var u = e as UnaryExpression;

                    if (TypeHelper.GetNonNullableType(u.Operand.Type) == TypeHelper.GetNonNullableType(type))
                    {
                        e = (e as UnaryExpression).Operand;
                    }
                }

                if (e.NodeType == ExpressionType.Constant)
                {
                    if (e.Type == type)
                    {
                        return e;
                    }
                    else if (TypeHelper.GetNonNullableType(e.Type) == TypeHelper.GetNonNullableType(type))
                    {
                        return Expression.Constant((e as ConstantExpression).Value, type);
                    }
                }

                if (e is MemberExpression me)
                {
                    if (me.Expression is ConstantExpression ce)
                    {
                        return this.PostEval(Expression.Constant(me.Member.GetValue(ce.Value), type));
                    }
                }

                if (type.GetTypeInfo().IsValueType)
                {
                    e = Expression.Convert(e, typeof(object));
                }

                var lambda = Expression.Lambda<Func<object>>(e);

                var fn = lambda.Compile();

                return this.PostEval(Expression.Constant(fn(), type));
            }
        }

        private class Nominator : ExpressionVisitor
        {
            private bool cannotBeEvaluated;
            private readonly HashSet<Expression> candidates;
            private readonly Func<Expression, bool> fnCanBeEvaluated;

            private Nominator(Func<Expression, bool> fnCanBeEvaluated)
            {
                this.candidates = new HashSet<Expression>();
                this.fnCanBeEvaluated = fnCanBeEvaluated;
            }

            public static HashSet<Expression> Nominate(Func<Expression, bool> fnCanBeEvaluated, Expression expression)
            {
                var nominator = new Nominator(fnCanBeEvaluated);

                if (nominator != null)
                {
                    nominator.Visit(expression);
                }

                return nominator.candidates;
            }

            protected override Expression VisitConstant(ConstantExpression c)
            {
                return base.VisitConstant(c);
            }

            protected override Expression Visit(Expression expression)
            {
                if (expression != null)
                {
                    var saveCannotBeEvaluated = this.cannotBeEvaluated; this.cannotBeEvaluated = false;

                    base.Visit(expression);

                    if (this.cannotBeEvaluated == false)
                    {
                        if (this.fnCanBeEvaluated(expression))
                        {
                            this.candidates.Add(expression);
                        }
                        else
                        {
                            this.cannotBeEvaluated = true;
                        }
                    }

                    this.cannotBeEvaluated |= saveCannotBeEvaluated;
                }

                return expression;
            }
        }
    }
}