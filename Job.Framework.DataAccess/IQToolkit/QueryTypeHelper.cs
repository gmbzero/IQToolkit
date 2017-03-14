using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace IQToolkit
{
    internal static class TypeHelper
    {
        public static Type FindIEnumerable(Type seqType)
        {
            if (seqType == null || seqType == typeof(string))
            {
                return null;
            }

            if (seqType.IsArray)
            {
                return typeof(IEnumerable<>).MakeGenericType(seqType.GetElementType());
            }

            var typeInfo = seqType.GetTypeInfo();

            if (typeInfo.IsGenericType)
            {
                foreach (var arg in seqType.GetGenericArguments())
                {
                    var ienum = typeof(IEnumerable<>).MakeGenericType(arg);

                    if (ienum.IsAssignableFrom(seqType))
                    {
                        return ienum;
                    }
                }
            }

            var ifaces = seqType.GetInterfaces();

            if (ifaces != null && ifaces.Length > 0)
            {
                foreach (var iface in ifaces)
                {
                    var ienum = FindIEnumerable(iface);

                    if (ienum != null)
                    {
                        return ienum;
                    }
                }
            }

            if (typeInfo.BaseType != null && typeInfo.BaseType != typeof(object))
            {
                return FindIEnumerable(typeInfo.BaseType);
            }

            return null;
        }

        public static Type GetSequenceType(Type elementType)
        {
            return typeof(IEnumerable<>).MakeGenericType(elementType);
        }

        public static Type GetElementType(Type seqType)
        {
            var ienum = FindIEnumerable(seqType);

            if (ienum == null)
            {
                return seqType;
            }

            return ienum.GetGenericArguments()[0];
        }

        public static bool IsNullableType(Type type)
        {
            return type != null && type.GetTypeInfo().IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        public static bool IsNullAssignable(Type type)
        {
            return type.GetTypeInfo().IsValueType == false || IsNullableType(type);
        }

        public static Type GetNonNullableType(Type type)
        {
            if (IsNullableType(type))
            {
                return type.GetGenericArguments()[0];
            }

            return type;
        }

        public static Type GetNullAssignableType(Type type)
        {
            if (IsNullAssignable(type) == false)
            {
                return typeof(Nullable<>).MakeGenericType(type);
            }

            return type;
        }

        public static ConstantExpression GetNullConstant(Type type)
        {
            return Expression.Constant(null, GetNullAssignableType(type));
        }

        public static Type GetMemberType(MemberInfo mi)
        {
            if (mi is FieldInfo fi) return fi.FieldType;
            if (mi is PropertyInfo pi) return pi.PropertyType;
            if (mi is EventInfo ei) return ei.EventHandlerType;
            if (mi is MethodInfo meth) return meth.ReturnType;

            return null;
        }

        public static object GetDefault(Type type)
        {
            var isNullable = type.GetTypeInfo().IsValueType == false || TypeHelper.IsNullableType(type);

            if (isNullable == false)
            {
                return Activator.CreateInstance(type);
            }

            return null;
        }

        public static bool IsReadOnly(MemberInfo member)
        {
            switch (member.MemberType)
            {
                case MemberTypes.Field:
                    {
                        var fi = member as FieldInfo;

                        return (fi.Attributes & FieldAttributes.InitOnly) != 0;
                    }
                case MemberTypes.Property:
                    {
                        var pi = member as PropertyInfo;

                        return pi.CanWrite == false || pi.GetSetMethod() == null;
                    }
                default:
                    {
                        return true;
                    }
            }
        }

        public static bool IsInteger(Type type)
        {
            var nnType = GetNonNullableType(type);

            switch (Type.GetTypeCode(nnType))
            {
                case TypeCode.SByte:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.Byte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    {
                        return true;
                    }
                default:
                    {
                        return false;
                    }
            }
        }
    }
}