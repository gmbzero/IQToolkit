using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace IQToolkit.Data.Common
{
    internal abstract class MappingEntity
    {
        public abstract string TableId { get; }

        public abstract Type ElementType { get; }

        public abstract Type EntityType { get; }
    }

    internal struct EntityInfo
    {
        public object Instance { get; }

        public MappingEntity Mapping { get; }

        public EntityInfo(object instance, MappingEntity mapping)
        {
            this.Instance = instance;
            this.Mapping = mapping;
        }
    }

    internal interface IHaveMappingEntity
    {
        MappingEntity Entity { get; }
    }

    internal abstract class QueryMapping
    {
        public virtual string GetTableId(Type type)
        {
            return type.Name;
        }

        public virtual MappingEntity GetEntity(Type type)
        {
            return this.GetEntity(type, this.GetTableId(type));
        }

        public abstract MappingEntity GetEntity(Type elementType, string entityID);

        public abstract MappingEntity GetEntity(MemberInfo contextMember);

        public abstract IEnumerable<MemberInfo> GetMappedMembers(MappingEntity entity);

        public abstract bool IsPrimaryKey(MappingEntity entity, MemberInfo member);

        public virtual IEnumerable<MemberInfo> GetPrimaryKeyMembers(MappingEntity entity)
        {
            return this.GetMappedMembers(entity).Where(m => this.IsPrimaryKey(entity, m));
        }

        public abstract bool IsRelationship(MappingEntity entity, MemberInfo member);

        public virtual bool IsSingletonRelationship(MappingEntity entity, MemberInfo member)
        {
            if (this.IsRelationship(entity, member) == false)
            {
                return false;
            }

            return TypeHelper.FindIEnumerable(TypeHelper.GetMemberType(member)) == null;
        }

        public virtual bool CanBeEvaluatedLocally(Expression expression)
        {
            if (expression is ConstantExpression cex)
            {
                if (cex.Value is IQueryable query && query.Provider == this)
                {
                    return false;
                }
            }

            if (expression is MethodCallExpression mc && (mc.Method.DeclaringType == typeof(Enumerable) || mc.Method.DeclaringType == typeof(Queryable) || mc.Method.DeclaringType == typeof(QueryUpdatable)))
            {
                return false;
            }

            if (expression.NodeType == ExpressionType.Convert && expression.Type == typeof(object))
            {
                return true;
            }

            return expression.NodeType != ExpressionType.Parameter && expression.NodeType != ExpressionType.Lambda;
        }

        public abstract object GetPrimaryKey(MappingEntity entity, object instance);

        public abstract Expression GetPrimaryKeyQuery(MappingEntity entity, Expression source, Expression[] keys);

        public abstract IEnumerable<EntityInfo> GetDependentEntities(MappingEntity entity, object instance);

        public abstract IEnumerable<EntityInfo> GetDependingEntities(MappingEntity entity, object instance);

        public abstract object CloneEntity(MappingEntity entity, object instance);

        public abstract bool IsModified(MappingEntity entity, object instance, object original);

        public abstract QueryMapper CreateMapper(QueryTranslator translator);
    }

    internal abstract class QueryMapper
    {
        public abstract QueryMapping Mapping { get; }

        public abstract QueryTranslator Translator { get; }

        internal abstract DbProjectionExpression GetQueryExpression(MappingEntity entity);
        
        internal abstract DbEntityExpression GetEntityExpression(Expression root, MappingEntity entity);
        
        public abstract Expression GetMemberExpression(Expression root, MappingEntity entity, MemberInfo member);
        
        public abstract Expression GetInsertExpression(MappingEntity entity, Expression instance, LambdaExpression selector);
        
        public abstract Expression GetUpdateExpression(MappingEntity entity, Expression instance, LambdaExpression updateCheck, LambdaExpression selector, Expression @else);
        
        public abstract Expression GetInsertOrUpdateExpression(MappingEntity entity, Expression instance, LambdaExpression updateCheck, LambdaExpression resultSelector);
        
        public abstract Expression GetDeleteExpression(MappingEntity entity, Expression instance, LambdaExpression deleteCheck);
        
        internal abstract DbEntityExpression IncludeMembers(DbEntityExpression entity, Func<MemberInfo, bool> fnIsIncluded);
        
        internal abstract bool HasIncludedMembers(DbEntityExpression entity);
        
        public virtual Expression ApplyMapping(Expression expression)
        {
            return DbQueryBinder.Bind(this, expression);
        }
        
        public virtual Expression Translate(Expression expression)
        {
            expression = DbQueryBinder.Bind(this, expression);          
            expression = DbAggregateRewriter.Rewrite(this.Translator.Linguist.Language, expression); 
            expression = DbUnusedColumnRemover.Remove(expression);
            expression = DbRedundantColumnRemover.Remove(expression);
            expression = DbRedundantSubqueryRemover.Remove(expression);
            expression = DbRedundantJoinRemover.Remove(expression);

            var bound = DbRelationshipBinder.Bind(this, expression);

            if (bound != expression)
            {
                expression = bound;
                expression = DbRedundantColumnRemover.Remove(expression);
                expression = DbRedundantJoinRemover.Remove(expression);
            }
            
            expression = DbComparisonRewriter.Rewrite(this.Mapping, expression);

            return expression;
        }
    }
}