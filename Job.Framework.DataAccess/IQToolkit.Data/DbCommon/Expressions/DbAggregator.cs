using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace IQToolkit.Data.Common
{
    internal static class DbAggregator
    {
        public static LambdaExpression GetAggregator(Type expectedType, Type actualType)
        {
            var actualElementType = TypeHelper.GetElementType(actualType);

            if (expectedType.IsAssignableFrom(actualType) == false)
            {
                var p = Expression.Parameter(actualType, "p");
                var body = null as Expression;
                var expectedElementType = TypeHelper.GetElementType(expectedType);

                if (expectedType.IsAssignableFrom(actualElementType))
                {
                    body = Expression.Call(typeof(Enumerable), "SingleOrDefault", new Type[] { actualElementType }, p);
                }
                else if (expectedType.GetTypeInfo().IsGenericType && (expectedType == typeof(IQueryable) || expectedType == typeof(IOrderedQueryable) || expectedType.GetGenericTypeDefinition() == typeof(IQueryable<>) || expectedType.GetGenericTypeDefinition() == typeof(IOrderedQueryable<>)))
                {
                    body = Expression.Call(typeof(Queryable), "AsQueryable", new Type[] { expectedElementType }, CoerceElement(expectedElementType, p));

                    if (body.Type != expectedType)
                    {
                        body = Expression.Convert(body, expectedType);
                    }
                }
                else if (expectedType.IsArray && expectedType.GetArrayRank() == 1)
                {
                    body = Expression.Call(typeof(Enumerable), "ToArray", new Type[] { expectedElementType }, CoerceElement(expectedElementType, p));
                }
                else if (expectedType.GetTypeInfo().IsGenericType && expectedType.GetGenericTypeDefinition().IsAssignableFrom(typeof(IList<>)))
                {
                    var gt = typeof(DeferredList<>).MakeGenericType(expectedType.GetGenericArguments());
                    var cn = gt.GetConstructor(new Type[] { typeof(IEnumerable<>).MakeGenericType(expectedType.GetGenericArguments()) });

                    body = Expression.New(cn, CoerceElement(expectedElementType, p));
                }
                else if (expectedType.IsAssignableFrom(typeof(List<>).MakeGenericType(actualElementType)))
                {
                    body = Expression.Call(typeof(Enumerable), "ToList", new Type[] { expectedElementType }, CoerceElement(expectedElementType, p));
                }
                else
                {
                    var ci = expectedType.GetConstructor(new Type[] { actualType });

                    if (ci != null)
                    {
                        body = Expression.New(ci, p);
                    }
                }
                if (body != null)
                {
                    return Expression.Lambda(body, p);
                }
            }

            return null;
        }

        private static Expression CoerceElement(Type expectedElementType, Expression expression)
        {
            var elementType = TypeHelper.GetElementType(expression.Type);

            if (expectedElementType != elementType && (expectedElementType.IsAssignableFrom(elementType) || elementType.IsAssignableFrom(expectedElementType)))
            {
                return Expression.Call(typeof(Enumerable), "Cast", new Type[] { expectedElementType }, expression);
            }

            return expression;
        }
    }
}