using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Reflection;

namespace IQToolkit
{
    internal abstract class ExpressionVisitor
    {
        protected virtual Expression Visit(Expression exp)
        {
            if (exp == null)
            {
                return exp;
            }

            switch (exp.NodeType)
            {
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                case ExpressionType.Not:
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                case ExpressionType.ArrayLength:
                case ExpressionType.Quote:
                case ExpressionType.TypeAs:
                case ExpressionType.UnaryPlus: return this.VisitUnary(exp as UnaryExpression);
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.Coalesce:
                case ExpressionType.ArrayIndex:
                case ExpressionType.RightShift:
                case ExpressionType.LeftShift:
                case ExpressionType.ExclusiveOr:
                case ExpressionType.Power: return this.VisitBinary(exp as BinaryExpression);
                case ExpressionType.TypeIs: return this.VisitTypeIs(exp as TypeBinaryExpression);
                case ExpressionType.Conditional: return this.VisitConditional((ConditionalExpression)exp as ConditionalExpression);
                case ExpressionType.Constant: return this.VisitConstant(exp as ConstantExpression);
                case ExpressionType.Parameter: return this.VisitParameter(exp as ParameterExpression);
                case ExpressionType.MemberAccess: return this.VisitMemberAccess(exp as MemberExpression);
                case ExpressionType.Call: return this.VisitMethodCall(exp as MethodCallExpression);
                case ExpressionType.Lambda: return this.VisitLambda(exp as LambdaExpression);
                case ExpressionType.New: return this.VisitNew(exp as NewExpression);
                case ExpressionType.NewArrayInit:
                case ExpressionType.NewArrayBounds: return this.VisitNewArray(exp as NewArrayExpression);
                case ExpressionType.Invoke: return this.VisitInvocation(exp as InvocationExpression);
                case ExpressionType.MemberInit: return this.VisitMemberInit(exp as MemberInitExpression);
                case ExpressionType.ListInit: return this.VisitListInit(exp as ListInitExpression);
                default:
                    {
                        return this.VisitUnknown(exp);
                    }
            }
        }

        protected virtual Expression VisitUnknown(Expression expression)
        {
            throw new Exception($"解析表达式目录树异常，不支持表达式 { expression.NodeType } 节点类型");
        }

        protected virtual MemberBinding VisitBinding(MemberBinding binding)
        {
            switch (binding.BindingType)
            {
                case MemberBindingType.Assignment: return this.VisitMemberAssignment(binding as MemberAssignment);
                case MemberBindingType.MemberBinding: return this.VisitMemberMemberBinding(binding as MemberMemberBinding);
                case MemberBindingType.ListBinding: return this.VisitMemberListBinding(binding as MemberListBinding);
                default:
                    {
                        throw new Exception($"解析表达式目录树异常，不支持表达式 { binding.BindingType } 绑定类型");
                    }
            }
        }

        protected virtual ElementInit VisitElementInitializer(ElementInit initializer)
        {
            var arguments = this.VisitExpressionList(initializer.Arguments);

            if (arguments != initializer.Arguments)
            {
                return Expression.ElementInit(initializer.AddMethod, arguments);
            }

            return initializer;
        }

        protected virtual Expression VisitUnary(UnaryExpression u)
        {
            return this.UpdateUnary(u, this.Visit(u.Operand), u.Type, u.Method);
        }

        protected virtual UnaryExpression UpdateUnary(UnaryExpression u, Expression operand, Type resultType, MethodInfo method)
        {
            if (u.Operand != operand || u.Type != resultType || u.Method != method)
            {
                return Expression.MakeUnary(u.NodeType, operand, resultType, method);
            }

            return u;
        }

        protected virtual Expression VisitBinary(BinaryExpression b)
        {
            var left = this.Visit(b.Left);
            var right = this.Visit(b.Right);
            var conversion = this.Visit(b.Conversion);

            return this.UpdateBinary(b, left, right, conversion, b.IsLiftedToNull, b.Method);
        }

        protected virtual BinaryExpression UpdateBinary(BinaryExpression b, Expression left, Expression right, Expression conversion, bool isLiftedToNull, MethodInfo method)
        {
            if (left != b.Left || right != b.Right || conversion != b.Conversion || method != b.Method || isLiftedToNull != b.IsLiftedToNull)
            {
                if (b.NodeType == ExpressionType.Coalesce && b.Conversion != null)
                {
                    return Expression.Coalesce(left, right, conversion as LambdaExpression);
                }
                else
                {
                    return Expression.MakeBinary(b.NodeType, left, right, isLiftedToNull, method);
                }
            }

            return b;
        }

        protected virtual Expression VisitTypeIs(TypeBinaryExpression b)
        {
            return this.UpdateTypeIs(b, this.Visit(b.Expression), b.TypeOperand);
        }

        protected virtual TypeBinaryExpression UpdateTypeIs(TypeBinaryExpression b, Expression expression, Type typeOperand)
        {
            if (expression != b.Expression || typeOperand != b.TypeOperand)
            {
                return Expression.TypeIs(expression, typeOperand);
            }

            return b;
        }

        protected virtual Expression VisitConstant(ConstantExpression c)
        {
            return c;
        }

        protected virtual Expression VisitConditional(ConditionalExpression c)
        {
            var test = this.Visit(c.Test);
            var ifTrue = this.Visit(c.IfTrue);
            var ifFalse = this.Visit(c.IfFalse);

            return this.UpdateConditional(c, test, ifTrue, ifFalse);
        }

        protected virtual ConditionalExpression UpdateConditional(ConditionalExpression c, Expression test, Expression ifTrue, Expression ifFalse)
        {
            if (test != c.Test || ifTrue != c.IfTrue || ifFalse != c.IfFalse)
            {
                return Expression.Condition(test, ifTrue, ifFalse);
            }

            return c;
        }

        protected virtual Expression VisitParameter(ParameterExpression p)
        {
            return p;
        }

        protected virtual Expression VisitMemberAccess(MemberExpression m)
        {
            return this.UpdateMemberAccess(m, this.Visit(m.Expression), m.Member);
        }

        protected virtual MemberExpression UpdateMemberAccess(MemberExpression m, Expression expression, MemberInfo member)
        {
            if (expression != m.Expression || member != m.Member)
            {
                return Expression.MakeMemberAccess(expression, member);
            }

            return m;
        }

        protected virtual Expression VisitMethodCall(MethodCallExpression m)
        {
            var obj = this.Visit(m.Object);
            var args = this.VisitExpressionList(m.Arguments);

            return this.UpdateMethodCall(m, obj, m.Method, args);
        }

        protected virtual MethodCallExpression UpdateMethodCall(MethodCallExpression m, Expression obj, MethodInfo method, IEnumerable<Expression> args)
        {
            if (obj != m.Object || method != m.Method || args != m.Arguments)
            {
                return Expression.Call(obj, method, args);
            }

            return m;
        }

        protected virtual ReadOnlyCollection<Expression> VisitExpressionList(ReadOnlyCollection<Expression> original)
        {
            if (original != null)
            {
                var list = null as List<Expression>;

                for (int i = 0, n = original.Count; i < n; i++)
                {
                    var p = this.Visit(original[i]);

                    if (list != null)
                    {
                        list.Add(p);
                    }

                    if (list == null && p != original[i])
                    {
                        list = new List<Expression>(n);

                        for (var j = 0; j < i; j++)
                        {
                            list.Add(original[j]);
                        }

                        list.Add(p);
                    }
                }

                if (list != null)
                {
                    return list.AsReadOnly();
                }
            }

            return original;
        }

        protected virtual ReadOnlyCollection<Expression> VisitMemberAndExpressionList(ReadOnlyCollection<MemberInfo> members, ReadOnlyCollection<Expression> original)
        {
            if (original != null)
            {
                var list = null as List<Expression>;

                for (int i = 0, n = original.Count; i < n; i++)
                {
                    var p = this.VisitMemberAndExpression(members?[i], original[i]);

                    if (list != null)
                    {
                        list.Add(p);
                    }

                    if (list == null && p != original[i])
                    {
                        list = new List<Expression>(n);

                        for (var j = 0; j < i; j++)
                        {
                            list.Add(original[j]);
                        }

                        list.Add(p);
                    }
                }

                if (list != null)
                {
                    return list.AsReadOnly();
                }
            }

            return original;
        }

        protected virtual Expression VisitMemberAndExpression(MemberInfo member, Expression expression)
        {
            return this.Visit(expression);
        }

        protected virtual MemberAssignment VisitMemberAssignment(MemberAssignment assignment)
        {
            return this.UpdateMemberAssignment(assignment, assignment.Member, this.Visit(assignment.Expression));
        }

        protected virtual MemberAssignment UpdateMemberAssignment(MemberAssignment assignment, MemberInfo member, Expression expression)
        {
            if (expression != assignment.Expression || member != assignment.Member)
            {
                return Expression.Bind(member, expression);
            }

            return assignment;
        }

        protected virtual MemberMemberBinding VisitMemberMemberBinding(MemberMemberBinding binding)
        {
            return this.UpdateMemberMemberBinding(binding, binding.Member, this.VisitBindingList(binding.Bindings));
        }

        protected virtual MemberMemberBinding UpdateMemberMemberBinding(MemberMemberBinding binding, MemberInfo member, IEnumerable<MemberBinding> bindings)
        {
            if (bindings != binding.Bindings || member != binding.Member)
            {
                return Expression.MemberBind(member, bindings);
            }

            return binding;
        }

        protected virtual MemberListBinding VisitMemberListBinding(MemberListBinding binding)
        {
            return this.UpdateMemberListBinding(binding, binding.Member, this.VisitElementInitializerList(binding.Initializers));
        }

        protected virtual MemberListBinding UpdateMemberListBinding(MemberListBinding binding, MemberInfo member, IEnumerable<ElementInit> initializers)
        {
            if (initializers != binding.Initializers || member != binding.Member)
            {
                return Expression.ListBind(member, initializers);
            }

            return binding;
        }

        protected virtual IEnumerable<MemberBinding> VisitBindingList(ReadOnlyCollection<MemberBinding> original)
        {
            var list = null as List<MemberBinding>;

            for (int i = 0, n = original.Count; i < n; i++)
            {
                var b = this.VisitBinding(original[i]);

                if (list != null)
                {
                    list.Add(b);
                }

                if (list == null && b != original[i])
                {
                    list = new List<MemberBinding>(n);

                    for (int j = 0; j < i; j++)
                    {
                        list.Add(original[j]);
                    }

                    list.Add(b);
                }
            }

            if (list != null)
            {
                return list;
            }

            return original;
        }

        protected virtual IEnumerable<ElementInit> VisitElementInitializerList(ReadOnlyCollection<ElementInit> original)
        {
            var list = null as List<ElementInit>;

            for (int i = 0, n = original.Count; i < n; i++)
            {
                var init = this.VisitElementInitializer(original[i]);

                if (list != null)
                {
                    list.Add(init);
                }

                if (list == null && init != original[i])
                {
                    list = new List<ElementInit>(n);

                    for (int j = 0; j < i; j++)
                    {
                        list.Add(original[j]);
                    }

                    list.Add(init);
                }
            }

            if (list != null)
            {
                return list;
            }

            return original;
        }

        protected virtual Expression VisitLambda(LambdaExpression lambda)
        {
            return this.UpdateLambda(lambda, lambda.Type, this.Visit(lambda.Body), lambda.Parameters);
        }

        protected virtual LambdaExpression UpdateLambda(LambdaExpression lambda, Type delegateType, Expression body, IEnumerable<ParameterExpression> parameters)
        {
            if (body != lambda.Body || parameters != lambda.Parameters || delegateType != lambda.Type)
            {
                return Expression.Lambda(delegateType, body, parameters);
            }

            return lambda;
        }

        protected virtual NewExpression VisitNew(NewExpression nex)
        {
            return this.UpdateNew(nex, nex.Constructor, this.VisitMemberAndExpressionList(nex.Members, nex.Arguments), nex.Members);
        }

        protected virtual NewExpression UpdateNew(NewExpression nex, ConstructorInfo constructor, IEnumerable<Expression> args, IEnumerable<MemberInfo> members)
        {
            if (args != nex.Arguments || constructor != nex.Constructor || members != nex.Members)
            {
                if (nex.Members != null)
                {
                    return Expression.New(constructor, args, members);
                }
                else
                {
                    return Expression.New(constructor, args);
                }
            }

            return nex;
        }

        protected virtual Expression VisitMemberInit(MemberInitExpression init)
        {
            var n = this.VisitNew(init.NewExpression);
            var bindings = this.VisitBindingList(init.Bindings);

            return this.UpdateMemberInit(init, n, bindings);
        }

        protected virtual MemberInitExpression UpdateMemberInit(MemberInitExpression init, NewExpression nex, IEnumerable<MemberBinding> bindings)
        {
            if (nex != init.NewExpression || bindings != init.Bindings)
            {
                return Expression.MemberInit(nex, bindings);
            }

            return init;
        }

        protected virtual Expression VisitListInit(ListInitExpression init)
        {
            var n = this.VisitNew(init.NewExpression);
            var initializers = this.VisitElementInitializerList(init.Initializers);

            return this.UpdateListInit(init, n, initializers);
        }

        protected virtual ListInitExpression UpdateListInit(ListInitExpression init, NewExpression nex, IEnumerable<ElementInit> initializers)
        {
            if (nex != init.NewExpression || initializers != init.Initializers)
            {
                return Expression.ListInit(nex, initializers);
            }

            return init;
        }

        protected virtual Expression VisitNewArray(NewArrayExpression na)
        {
            return this.UpdateNewArray(na, na.Type, this.VisitExpressionList(na.Expressions));
        }

        protected virtual NewArrayExpression UpdateNewArray(NewArrayExpression na, Type arrayType, IEnumerable<Expression> expressions)
        {
            if (expressions != na.Expressions || na.Type != arrayType)
            {
                if (na.NodeType == ExpressionType.NewArrayInit)
                {
                    return Expression.NewArrayInit(arrayType.GetElementType(), expressions);
                }
                else
                {
                    return Expression.NewArrayBounds(arrayType.GetElementType(), expressions);
                }
            }

            return na;
        }

        protected virtual Expression VisitInvocation(InvocationExpression iv)
        {
            var args = this.VisitExpressionList(iv.Arguments);
            var expr = this.Visit(iv.Expression);

            return this.UpdateInvocation(iv, expr, args);
        }

        protected virtual InvocationExpression UpdateInvocation(InvocationExpression iv, Expression expression, IEnumerable<Expression> args)
        {
            if (args != iv.Arguments || expression != iv.Expression)
            {
                return Expression.Invoke(expression, args);
            }

            return iv;
        }
    }
}