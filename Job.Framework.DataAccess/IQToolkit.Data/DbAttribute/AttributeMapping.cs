using IQToolkit.Data.Common;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;

namespace IQToolkit.Data.Mapping
{
    public abstract class BaseAttribute : Attribute
    {

    }

    public abstract class TableBaseAttribute : BaseAttribute
    {
        public string Name { get; set; }

        public string Alias { get; set; }
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
    public sealed class TableAttribute : TableBaseAttribute
    {
        public Type EntityType { get; set; }
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = true)]
    public sealed class TableExtensionAttribute : TableBaseAttribute
    {
        public string KeyColumns { get; set; }

        public string RelatedAlias { get; set; }

        public string RelatedKeyColumns { get; set; }
    }

    public abstract class MemberAttribute : BaseAttribute
    {
        public string Member { get; set; }
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = true)]
    public class ColumnAttribute : MemberAttribute
    {
        public string Name { get; set; }

        public string Alias { get; set; }

        public string DbType { get; set; }

        public bool IsComputed { get; set; }

        public bool IsPrimaryKey { get; set; }

        public bool IsGenerated { get; set; }

        public bool IsReadOnly { get; set; }
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = true)]
    public class AssociationAttribute : MemberAttribute
    {
        public string Name { get; set; }

        public string KeyMembers { get; set; }

        public string RelatedEntityID { get; set; }

        public Type RelatedEntityType { get; set; }

        public string RelatedKeyMembers { get; set; }

        public bool IsForeignKey { get; set; }
    }

    internal class AttributeMapping : AdvancedMapping
    {
        private readonly Type contextType;
        private static readonly char[] dotSeparator = new char[] { '.' };
        private static readonly char[] separators = new char[] { ' ', ',', '|' };
        private static ConcurrentDictionary<string, MappingEntity> entities = new ConcurrentDictionary<string, MappingEntity>();

        public AttributeMapping(Type contextType)
        {
            this.contextType = contextType;
        }

        public override MappingEntity GetEntity(MemberInfo contextMember)
        {
            var elementType = TypeHelper.GetElementType(TypeHelper.GetMemberType(contextMember));

            return this.GetEntity(elementType, contextMember.Name);
        }

        public override MappingEntity GetEntity(Type type, string tableId)
        {
            return this.GetEntity(type, tableId, type);
        }

        private MappingEntity GetEntity(Type elementType, string tableId, Type entityType)
        {
            if (tableId == null)
            {
                tableId = this.GetTableId(elementType);
            }

            return entities.GetOrAdd(tableId, (key) =>
            {
                return this.CreateEntity(elementType, key, entityType);
            });
        }

        protected virtual IEnumerable<BaseAttribute> GetMappingAttributes(string rootEntityId)
        {
            var contextMember = this.FindMember(this.contextType, rootEntityId);

            return contextMember.GetCustomAttributes(typeof(BaseAttribute)) as BaseAttribute[];
        }

        public override string GetTableId(Type entityType)
        {
            if (contextType != null)
            {
                foreach (var mi in contextType.GetMembers(BindingFlags.Instance | BindingFlags.Public))
                {
                    if (mi is FieldInfo fi && TypeHelper.GetElementType(fi.FieldType) == entityType)
                    {
                        return fi.Name;
                    }

                    if (mi is PropertyInfo pi && TypeHelper.GetElementType(pi.PropertyType) == entityType)
                    {
                        return pi.Name;
                    }
                }
            }

            return entityType.Name;
        }

        private MappingEntity CreateEntity(Type elementType, string tableId, Type entityType)
        {
            if (tableId == null)
            {
                tableId = this.GetTableId(elementType);
            }

            var members = new HashSet<string>();
            var mappingMembers = new List<AttributeMappingMember>();
            int dot = tableId.IndexOf('.');
            var rootTableId = dot > 0 ? tableId.Substring(0, dot) : tableId;
            var path = dot > 0 ? tableId.Substring(dot + 1) : string.Empty;
            var mappingAttributes = this.GetMappingAttributes(rootTableId);
            var tableAttributes = mappingAttributes.OfType<TableBaseAttribute>().OrderBy(ta => ta.Name);
            var tableAttr = tableAttributes.OfType<TableAttribute>().FirstOrDefault();

            if (tableAttr != null && tableAttr.EntityType != null && entityType == elementType)
            {
                entityType = tableAttr.EntityType;
            }

            var memberAttributes = mappingAttributes.OfType<MemberAttribute>().Where(ma => ma.Member.StartsWith(path)).OrderBy(ma => ma.Member);

            foreach (var attr in memberAttributes)
            {
                if (string.IsNullOrEmpty(attr.Member))
                {
                    continue;
                }

                var memberName = path.Length == 0 ? attr.Member : attr.Member.Substring(path.Length + 1);
                var member = null as MemberInfo;
                var attribute = null as MemberAttribute;
                var nested = null as AttributeMappingEntity;

                if (memberName.Contains('.'))
                {
                    var nestedMember = memberName.Substring(0, memberName.IndexOf('.'));

                    if (nestedMember.Contains('.'))
                    {
                        continue;
                    }

                    if (members.Contains(nestedMember))
                    {
                        continue;
                    }

                    members.Add(nestedMember);

                    member = this.FindMember(entityType, nestedMember);

                    nested = this.GetEntity(TypeHelper.GetMemberType(member), tableId + "." + nestedMember) as AttributeMappingEntity;
                }
                else
                {
                    if (members.Contains(memberName))
                    {
                        throw new InvalidOperationException($"AttributeMapping: more than one mapping attribute specified for member '{ memberName }' on type '{ entityType.Name }'");
                    }

                    member = this.FindMember(entityType, memberName);

                    attribute = attr;
                }

                mappingMembers.Add(new AttributeMappingMember(member, attribute, nested));
            }

            return new AttributeMappingEntity(elementType, tableId, entityType, tableAttributes, mappingMembers);
        }

        private MemberInfo FindMember(Type type, string path)
        {
            var member = null as MemberInfo;
            var names = path.Split(dotSeparator);

            foreach (string name in names)
            {
                member = type.GetMember(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.IgnoreCase).FirstOrDefault();

                if (member == null)
                {
                    throw new InvalidOperationException($"AttributMapping: the member '{ name }' does not exist on type '{ type.Name }'");
                }

                type = TypeHelper.GetElementType(TypeHelper.GetMemberType(member));
            }

            return member;
        }

        public override string GetTableName(MappingEntity entity)
        {
            var en = entity as AttributeMappingEntity;
            var table = en.Tables.FirstOrDefault();

            return this.GetTableName(table);
        }

        private string GetTableName(MappingEntity entity, TableBaseAttribute attr)
        {
            return (attr != null && !string.IsNullOrEmpty(attr.Name)) ? attr.Name : entity.TableId;
        }

        public override IEnumerable<MemberInfo> GetMappedMembers(MappingEntity entity)
        {
            return (entity as AttributeMappingEntity).MappedMembers;
        }

        public override bool IsMapped(MappingEntity entity, MemberInfo member)
        {
            return (entity as AttributeMappingEntity).GetMappingMember(member.Name) != null;
        }

        public override bool IsColumn(MappingEntity entity, MemberInfo member)
        {
            var mm = (entity as AttributeMappingEntity).GetMappingMember(member.Name);

            if (mm == null)
            {
                return false;
            }

            return mm.Column != null;
        }

        public override bool IsComputed(MappingEntity entity, MemberInfo member)
        {
            var mm = (entity as AttributeMappingEntity).GetMappingMember(member.Name);

            if (mm == null)
            {
                return false;
            }

            return mm.Column != null && mm.Column.IsComputed;
        }

        public override bool IsGenerated(MappingEntity entity, MemberInfo member)
        {
            var mm = (entity as AttributeMappingEntity).GetMappingMember(member.Name);

            if (mm == null)
            {
                return false;
            }

            return mm.Column != null && mm.Column.IsGenerated;
        }

        public override bool IsReadOnly(MappingEntity entity, MemberInfo member)
        {
            var mm = (entity as AttributeMappingEntity).GetMappingMember(member.Name);

            if (mm == null)
            {
                return false;
            }

            return mm.Column != null && mm.Column.IsReadOnly;
        }

        public override bool IsPrimaryKey(MappingEntity entity, MemberInfo member)
        {
            var mm = (entity as AttributeMappingEntity).GetMappingMember(member.Name);

            if (mm == null)
            {
                return false;
            }

            return mm.Column != null && mm.Column.IsPrimaryKey;
        }

        public override string GetColumnName(MappingEntity entity, MemberInfo member)
        {
            var mm = (entity as AttributeMappingEntity).GetMappingMember(member.Name);

            if (mm != null && mm.Column != null && !string.IsNullOrWhiteSpace(mm.Column.Name))
            {
                return mm.Column.Name;
            }

            return base.GetColumnName(entity, member);
        }

        public override string GetColumnDbType(MappingEntity entity, MemberInfo member)
        {
            var mm = (entity as AttributeMappingEntity).GetMappingMember(member.Name);

            if (mm != null && mm.Column != null && !string.IsNullOrEmpty(mm.Column.DbType))
            {
                return mm.Column.DbType;
            }

            return null;
        }

        public override bool IsAssociationRelationship(MappingEntity entity, MemberInfo member)
        {
            var mm = (entity as AttributeMappingEntity).GetMappingMember(member.Name);

            if (mm == null)
            {
                return false;
            }

            return mm.Association != null;
        }

        public override bool IsRelationshipSource(MappingEntity entity, MemberInfo member)
        {
            var mm = (entity as AttributeMappingEntity).GetMappingMember(member.Name);

            if (mm != null && mm.Association != null)
            {
                if (mm.Association.IsForeignKey && !typeof(IEnumerable).IsAssignableFrom(TypeHelper.GetMemberType(member)))
                {
                    return true;
                }
            }

            return false;
        }

        public override bool IsRelationshipTarget(MappingEntity entity, MemberInfo member)
        {
            var mm = (entity as AttributeMappingEntity).GetMappingMember(member.Name);

            if (mm != null && mm.Association != null)
            {
                if (mm.Association.IsForeignKey == false || typeof(IEnumerable).IsAssignableFrom(TypeHelper.GetMemberType(member)))
                {
                    return true;
                }
            }

            return false;
        }

        public override bool IsNestedEntity(MappingEntity entity, MemberInfo member)
        {
            var mm = (entity as AttributeMappingEntity).GetMappingMember(member.Name);

            if (mm == null)
            {
                return false;
            }

            return mm.NestedEntity != null;
        }

        public override MappingEntity GetRelatedEntity(MappingEntity entity, MemberInfo member)
        {
            var thisEntity = entity as AttributeMappingEntity;
            var mm = thisEntity.GetMappingMember(member.Name);

            if (mm != null)
            {
                if (mm.Association != null)
                {
                    var elementType = TypeHelper.GetElementType(TypeHelper.GetMemberType(member));
                    var entityType = mm.Association.RelatedEntityType ?? elementType;

                    return this.GetReferencedEntity(elementType, mm.Association.RelatedEntityID, entityType, "Association.RelatedEntityID");
                }
                else if (mm.NestedEntity != null)
                {
                    return mm.NestedEntity;
                }
            }

            return base.GetRelatedEntity(entity, member);
        }

        public override IEnumerable<MemberInfo> GetAssociationKeyMembers(MappingEntity entity, MemberInfo member)
        {
            var thisEntity = entity as AttributeMappingEntity;
            var mm = thisEntity.GetMappingMember(member.Name);

            if (mm != null && mm.Association != null)
            {
                return this.GetReferencedMembers(thisEntity, mm.Association.KeyMembers, "Association.KeyMembers", thisEntity.EntityType);
            }

            return base.GetAssociationKeyMembers(entity, member);
        }

        public override IEnumerable<MemberInfo> GetAssociationRelatedKeyMembers(MappingEntity entity, MemberInfo member)
        {
            var thisEntity = entity as AttributeMappingEntity;
            var relatedEntity = this.GetRelatedEntity(entity, member) as AttributeMappingEntity;
            var mm = thisEntity.GetMappingMember(member.Name);

            if (mm != null && mm.Association != null)
            {
                return this.GetReferencedMembers(relatedEntity, mm.Association.RelatedKeyMembers, "Association.RelatedKeyMembers", thisEntity.EntityType);
            }

            return base.GetAssociationRelatedKeyMembers(entity, member);
        }

        private IEnumerable<MemberInfo> GetReferencedMembers(AttributeMappingEntity entity, string names, string source, Type sourceType)
        {
            return names.Split(separators).Select(n => this.GetReferencedMember(entity, n, source, sourceType));
        }

        private MemberInfo GetReferencedMember(AttributeMappingEntity entity, string name, string source, Type sourceType)
        {
            var mm = entity.GetMappingMember(name);

            if (mm == null)
            {
                throw new InvalidOperationException($"AttributeMapping: The member '{ entity.EntityType.Name }.{ name }' referenced in { source } for '{ sourceType.Name }' is not mapped or does not exist");
            }

            return mm.Member;
        }

        private MappingEntity GetReferencedEntity(Type elementType, string name, Type entityType, string source)
        {
            var entity = this.GetEntity(elementType, name, entityType);

            if (entity == null)
            {
                throw new InvalidOperationException($"The entity '{ name }' referenced in { source } of '{ entityType.Name }' does not exist");
            }

            return entity;
        }

        public override IList<MappingTable> GetTables(MappingEntity entity)
        {
            return (entity as AttributeMappingEntity).Tables;
        }

        public override string GetAlias(MappingTable table)
        {
            return (table as AttributeMappingTable).Attribute.Alias;
        }

        public override string GetAlias(MappingEntity entity, MemberInfo member)
        {
            var mm = (entity as AttributeMappingEntity).GetMappingMember(member.Name);

            if (mm != null && mm.Column != null)
            {
                return mm.Column.Alias;
            }

            return null;
        }

        public override string GetTableName(MappingTable table)
        {
            var amt = table as AttributeMappingTable;

            return this.GetTableName(amt.Entity, amt.Attribute);
        }

        public override bool IsExtensionTable(MappingTable table)
        {
            return (table as AttributeMappingTable).Attribute is TableExtensionAttribute;
        }

        public override string GetExtensionRelatedAlias(MappingTable table)
        {
            var attr = (table as AttributeMappingTable).Attribute as TableExtensionAttribute;

            return attr?.RelatedAlias;
        }

        public override IEnumerable<string> GetExtensionKeyColumnNames(MappingTable table)
        {
            var attr = (table as AttributeMappingTable).Attribute as TableExtensionAttribute;

            if (attr == null)
            {
                return new string[] { };
            }

            return attr.KeyColumns.Split(separators);
        }

        public override IEnumerable<MemberInfo> GetExtensionRelatedMembers(MappingTable table)
        {
            var amt = table as AttributeMappingTable;
            var attr = amt.Attribute as TableExtensionAttribute;

            if (attr == null)
            {
                return new MemberInfo[] { };
            }

            return attr.RelatedKeyColumns.Split(separators).Select(n => this.GetMemberForColumn(amt.Entity, n));
        }

        private MemberInfo GetMemberForColumn(MappingEntity entity, string columnName)
        {
            foreach (var m in this.GetMappedMembers(entity))
            {
                if (this.IsNestedEntity(entity, m))
                {
                    var m2 = this.GetMemberForColumn(this.GetRelatedEntity(entity, m), columnName);

                    if (m2 != null)
                    {
                        return m2;
                    }
                }
                else if (this.IsColumn(entity, m) && string.Compare(this.GetColumnName(entity, m), columnName, true) == 0)
                {
                    return m;
                }
            }

            return null;
        }

        public override QueryMapper CreateMapper(QueryTranslator translator)
        {
            return new AttributeMapper(this, translator);
        }

        private class AttributeMapper : AdvancedMapper
        {
            private readonly AttributeMapping mapping;

            public AttributeMapper(AttributeMapping mapping, QueryTranslator translator) : base(mapping, translator)
            {
                this.mapping = mapping;
            }
        }

        private class AttributeMappingMember
        {
            private readonly MemberInfo member;
            private readonly MemberAttribute attribute;
            private readonly AttributeMappingEntity nested;

            public MemberInfo Member
            {
                get { return this.member; }
            }

            public ColumnAttribute Column
            {
                get { return this.attribute as ColumnAttribute; }
            }

            public AssociationAttribute Association
            {
                get { return this.attribute as AssociationAttribute; }
            }

            public AttributeMappingEntity NestedEntity
            {
                get { return this.nested; }
            }

            public AttributeMappingMember(MemberInfo member, MemberAttribute attribute, AttributeMappingEntity nested)
            {
                this.member = member;
                this.attribute = attribute;
                this.nested = nested;
            }
        }

        private class AttributeMappingTable : MappingTable
        {
            private readonly AttributeMappingEntity entity;
            private readonly TableBaseAttribute attribute;

            public AttributeMappingEntity Entity
            {
                get { return this.entity; }
            }

            public TableBaseAttribute Attribute
            {
                get { return this.attribute; }
            }

            public AttributeMappingTable(AttributeMappingEntity entity, TableBaseAttribute attribute)
            {
                this.entity = entity;
                this.attribute = attribute;
            }
        }

        private class AttributeMappingEntity : MappingEntity
        {
            private readonly string tableId;
            private readonly Type elementType;
            private readonly Type entityType;
            private readonly ReadOnlyCollection<MappingTable> tables;
            private readonly Dictionary<string, AttributeMappingMember> mappingMembers;

            public override string TableId
            {
                get { return this.tableId; }
            }

            public override Type ElementType
            {
                get { return this.elementType; }
            }

            public override Type EntityType
            {
                get { return this.entityType; }
            }

            public ReadOnlyCollection<MappingTable> Tables
            {
                get { return this.tables; }
            }

            public AttributeMappingMember GetMappingMember(string name)
            {
                this.mappingMembers.TryGetValue(name, out AttributeMappingMember mm);

                return mm;
            }

            public IEnumerable<MemberInfo> MappedMembers
            {
                get { return this.mappingMembers.Values.Select(mm => mm.Member); }
            }

            public AttributeMappingEntity(Type elementType, string tableId, Type entityType, IEnumerable<TableBaseAttribute> attrs, IEnumerable<AttributeMappingMember> mappingMembers)
            {
                this.tableId = tableId;
                this.elementType = elementType;
                this.entityType = entityType;
                this.tables = attrs.Select(a => new AttributeMappingTable(this, a) as MappingTable).ToReadOnly();
                this.mappingMembers = mappingMembers.ToDictionary(mm => mm.Member.Name);
            }
        }
    }
}