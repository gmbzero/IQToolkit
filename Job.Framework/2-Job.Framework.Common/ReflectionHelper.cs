using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Job.Framework.Common
{
    /// <summary>
    /// 构造高效的反射方式帮助类
    /// </summary>
    public static class ReflectionHelper
    {
        /// <summary>
        /// 判断类型是否继承于此泛型
        /// </summary>
        /// <param name="to">需要判断的类型</param>
        /// <param name="from">泛型定义类型</param>
        /// <returns>返回判断结果</returns>
        public static bool IsGenericType(Type to, Type from)
        {
            var t = to.GetTypeInfo();
            var f = from.GetTypeInfo();

            if (t.IsGenericType == false || f.IsGenericType == false)
            {
                return false;
            }

            return t.GetGenericTypeDefinition() == from;
        }

        public static Type GetGenericType(Type seqType)
        {
            var ienum = FindIEnumerable(seqType);

            if (ienum == null)
            {
                return seqType;
            }

            return ienum.GetGenericArguments()[0];
        }

        private static Type FindIEnumerable(Type seqType)
        {
            if (seqType == null || seqType == typeof(string))
            {
                return null;
            }

            if (seqType.IsArray)
            {
                return typeof(IEnumerable<>).MakeGenericType(seqType.GetElementType());
            }

            if (seqType.GetTypeInfo().IsGenericType)
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

            var baseType = seqType.GetTypeInfo().BaseType;

            if (baseType != null && baseType != typeof(object))
            {
                return FindIEnumerable(baseType);
            }

            return null;
        }

        /// <summary>
        /// 缓存获取模型时的属性元数据数组
        /// </summary>
        private static ConcurrentDictionary<string, PropertyInfo[]> PropertyConcurrentDic = new ConcurrentDictionary<string, PropertyInfo[]>();

        /// <summary>
        /// 获取属性元数据集合
        /// </summary>
        /// <param name="obj">需要解析的匿名对象</param>
        /// <returns>返回属性元数据集合</returns>
        public static PropertyInfo[] GetProperties(object obj)
        {
            return GetProperties(obj.GetType());
        }

        /// <summary>
        /// 获取属性元数据集合
        /// </summary>
        /// <param name="type">需要解析的类型对象</param>
        /// <returns>返回属性元数据集合</returns>
        public static PropertyInfo[] GetProperties(Type type)
        {
            var properties = PropertyConcurrentDic.GetOrAdd(type.AssemblyQualifiedName, (s) =>
            {
                return type.GetProperties();
            });

            return properties;
        }

        /// <summary>
        /// 缓存获取字段值时高效的表达式目录树
        /// </summary>
        private static ConcurrentDictionary<string, Func<object, object>> FieldGetConcurrentDic = new ConcurrentDictionary<string, Func<object, object>>();

        /// <summary>
        /// 获取字段值，高效的表达式目录树形式
        /// </summary>
        /// <param name="field">需要操作的字段</param>
        /// <returns>表达式目录树</returns>
        public static Func<object, object> CreateGetFieldLambda(this FieldInfo field)
        {
            var func = FieldGetConcurrentDic.GetOrAdd(field.DeclaringType.AssemblyQualifiedName + "." + field.Name, (s) =>
            {
                var target = Expression.Parameter(typeof(object));
                var castTarget = Expression.Convert(target, field.DeclaringType);
                var method = Expression.Call(Expression.Constant(field), typeof(FieldInfo).GetMethod("GetValue", new Type[] { typeof(object) }), castTarget);
                var lambda = Expression.Lambda<Func<object, object>>(method, new[] { target });

                return lambda.Compile();
            });

            return func;
        }

        /// <summary>
        /// 缓存设置属性值时高效的表达式目录树
        /// </summary>
        private static ConcurrentDictionary<string, Action<object, object>> FieldSetConcurrentDic = new ConcurrentDictionary<string, Action<object, object>>();

        /// <summary>
        /// 设置字段值，高效的表达式目录树形式
        /// </summary>
        /// <param name="field">需要操作的字段</param>
        /// <returns>表达式目录树</returns>
        public static Action<object, object> CreateSetFieldLambda(this FieldInfo field)
        {
            var act = FieldSetConcurrentDic.GetOrAdd(field.DeclaringType.AssemblyQualifiedName + "." + field.Name, (s) =>
            {
                var target = Expression.Parameter(typeof(object));
                var fieldValue = Expression.Parameter(typeof(object));
                var castTarget = Expression.Convert(target, field.DeclaringType);
                var castfieldValue = Expression.Convert(fieldValue, field.FieldType);
                var method = Expression.Call(Expression.Constant(field), typeof(FieldInfo).GetMethod("SetValue", new Type[] { typeof(object), typeof(object) }), castTarget, castfieldValue);
                var lambda = Expression.Lambda<Action<object, object>>(method, new[] { target, fieldValue });

                return lambda.Compile();
            });

            return act;
        }

        /// <summary>
        /// 缓存获取属性值时高效的表达式目录树
        /// </summary>
        private static ConcurrentDictionary<string, Func<object, object>> PropertyGetConcurrentDic = new ConcurrentDictionary<string, Func<object, object>>();

        /// <summary>
        /// 获取属性值，高效的表达式目录树形式
        /// </summary>
        /// <param name="property">需要操作的属性</param>
        /// <returns>表达式目录树</returns>
        public static Func<object, object> CreateGetPropertyLambda(this PropertyInfo property)
        {
            var func = PropertyGetConcurrentDic.GetOrAdd(property.DeclaringType.AssemblyQualifiedName + "." + property.Name, (s) =>
            {
                var target = Expression.Parameter(typeof(object));
                var castTarget = Expression.Convert(target, property.DeclaringType);
                var propertyValue = Expression.Property(castTarget, property);
                var castPropertyValue = Expression.Convert(propertyValue, typeof(object));
                var lambda = Expression.Lambda<Func<object, object>>(castPropertyValue, target);

                return lambda.Compile();
            });

            return func;
        }

        /// <summary>
        /// 缓存设置属性值时高效的表达式目录树
        /// </summary>
        private static ConcurrentDictionary<string, Action<object, object>> PropertySetConcurrentDic = new ConcurrentDictionary<string, Action<object, object>>();

        /// <summary>
        /// 设置属性值，高效的表达式目录树形式
        /// </summary>
        /// <param name="property">需要操作的属性</param>
        /// <returns>表达式目录树</returns>
        public static Action<object, object> CreateSetPropertyLambda(this PropertyInfo property)
        {
            var act = PropertySetConcurrentDic.GetOrAdd(property.DeclaringType.AssemblyQualifiedName + "." + property.Name, (s) =>
            {
                var target = Expression.Parameter(typeof(object));
                var propertyValue = Expression.Parameter(typeof(object));
                var castTarget = Expression.Convert(target, property.DeclaringType);
                var castPropertyValue = Expression.Convert(propertyValue, property.PropertyType);
                var method = Expression.Call(castTarget, property.GetSetMethod(), castPropertyValue);
                var lambda = Expression.Lambda<Action<object, object>>(method, new[] { target, propertyValue });

                return lambda.Compile();
            });

            return act;
        }

        /// <summary>
        /// 一个用于深度拷贝对象实例方法
        /// </summary>
        /// <param name="type">对象类型</param>
        /// <returns>返回一个尝试拷贝的对象实例</returns>
        public static T CreateInstance<T>(Type type)
        {
            return Expression.Lambda<Func<T>>(Expression.New(type), null).Compile()();
        }
    }
}
