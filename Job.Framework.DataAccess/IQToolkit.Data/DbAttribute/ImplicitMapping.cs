using IQToolkit.Data.Common;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace IQToolkit.Data.Mapping
{
    internal class ImplicitMapping : BasicMapping
    {
        public override string GetTableId(Type type)
        {
            return this.InferTableName(type);
        }

        public override bool IsPrimaryKey(MappingEntity entity, MemberInfo member)
        {
            if (this.IsColumn(entity, member))
            {
                var name = NameWithoutTrailingDigits(member.Name);

                return member.Name.EndsWith("ID") && member.DeclaringType.Name.StartsWith(member.Name.Substring(0, member.Name.Length - 2));
            }

            return false;
        }

        private string NameWithoutTrailingDigits(string name)
        {
            var n = name.Length - 1;

            while (n >= 0 && char.IsDigit(name[n]))
            {
                n--;
            }

            if (n < name.Length - 1)
            {
                return name.Substring(0, n);
            }

            return name;
        }

        public override bool IsColumn(MappingEntity entity, MemberInfo member)
        {
            return IsScalar(TypeHelper.GetMemberType(member));
        }

        private bool IsScalar(Type type)
        {
            type = TypeHelper.GetNonNullableType(type);

            if (TypeHelper.IsNullableType(type))
            {
                return false;
            }

            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Empty:
                    return false;
                case TypeCode.Object:
                    return type == typeof(DateTimeOffset) || type == typeof(TimeSpan) || type == typeof(Guid) || type == typeof(byte[]);
                default:
                    return true;
            }
        }

        public override bool IsAssociationRelationship(MappingEntity entity, MemberInfo member)
        {
            if (IsMapped(entity, member) && !IsColumn(entity, member))
            {
                var otherType = TypeHelper.GetElementType(TypeHelper.GetMemberType(member));

                return this.IsScalar(otherType) == false;
            }

            return false;
        }

        public override bool IsRelationshipSource(MappingEntity entity, MemberInfo member)
        {
            if (IsAssociationRelationship(entity, member))
            {
                if (typeof(IEnumerable).IsAssignableFrom(TypeHelper.GetMemberType(member)))
                {
                    return false;
                }

                var entity2 = GetRelatedEntity(entity, member);
                var relatedPKs = new HashSet<string>(this.GetPrimaryKeyMembers(entity2).Select(m => m.Name));
                var relatedKeyMembers = new HashSet<string>(this.GetAssociationRelatedKeyMembers(entity, member).Select(m => m.Name));

                return relatedPKs.IsSubsetOf(relatedKeyMembers) && relatedKeyMembers.IsSubsetOf(relatedPKs);
            }

            return false;
        }

        public override bool IsRelationshipTarget(MappingEntity entity, MemberInfo member)
        {
            if (IsAssociationRelationship(entity, member))
            {
                if (typeof(IEnumerable).IsAssignableFrom(TypeHelper.GetMemberType(member)))
                {
                    return true;
                }

                var pks = new HashSet<string>(this.GetPrimaryKeyMembers(entity).Select(m => m.Name));
                var keys = new HashSet<string>(this.GetAssociationKeyMembers(entity, member).Select(m => m.Name));

                return keys.IsSubsetOf(pks) && pks.IsSubsetOf(keys);
            }

            return false;
        }

        public override IEnumerable<MemberInfo> GetAssociationKeyMembers(MappingEntity entity, MemberInfo member)
        {
            this.GetAssociationKeys(entity, member, out List<MemberInfo> keyMembers, out List<MemberInfo> relatedKeyMembers);

            return keyMembers;
        }

        public override IEnumerable<MemberInfo> GetAssociationRelatedKeyMembers(MappingEntity entity, MemberInfo member)
        {
            this.GetAssociationKeys(entity, member, out List<MemberInfo> keyMembers, out List<MemberInfo> relatedKeyMembers);

            return relatedKeyMembers;
        }

        private void GetAssociationKeys(MappingEntity entity, MemberInfo member, out List<MemberInfo> keyMembers, out List<MemberInfo> relatedKeyMembers)
        {
            var entity2 = GetRelatedEntity(entity, member);
            var map1 = this.GetMappedMembers(entity).Where(m => this.IsColumn(entity, m)).ToDictionary(m => m.Name);
            var map2 = this.GetMappedMembers(entity2).Where(m => this.IsColumn(entity2, m)).ToDictionary(m => m.Name);
            var commonNames = map1.Keys.Intersect(map2.Keys).OrderBy(k => k);

            keyMembers = new List<MemberInfo>();
            relatedKeyMembers = new List<MemberInfo>();

            foreach (string name in commonNames)
            {
                keyMembers.Add(map1[name]);
                relatedKeyMembers.Add(map2[name]);
            }
        }

        public override string GetTableName(MappingEntity entity)
        {
            return string.IsNullOrEmpty(entity.TableId) == false ? entity.TableId : this.InferTableName(entity.EntityType);
        }

        private string InferTableName(Type rowType)
        {
            return SplitWords(Plural(rowType.Name));
        }

        public static string SplitWords(string name)
        {
            var sb = null as StringBuilder;
            var lastIsLower = char.IsLower(name[0]);

            for (int i = 0, n = name.Length; i < n; i++)
            {
                var thisIsLower = char.IsLower(name[i]);

                if (lastIsLower && !thisIsLower)
                {
                    if (sb == null)
                    {
                        sb = new StringBuilder();
                        sb.Append(name, 0, i);
                    }

                    sb.Append(" ");
                }

                if (sb != null)
                {
                    sb.Append(name[i]);
                }

                lastIsLower = thisIsLower;
            }

            if (sb != null)
            {
                return sb.ToString();
            }

            return name;
        }

        public static string Plural(string name)
        {
            if (name.EndsWith("x", StringComparison.CurrentCultureIgnoreCase) || name.EndsWith("ch", StringComparison.CurrentCultureIgnoreCase) || name.EndsWith("ss", StringComparison.CurrentCultureIgnoreCase))
            {
                return name + "es";
            }
            else if (name.EndsWith("y", StringComparison.CurrentCultureIgnoreCase))
            {
                return name.Substring(0, name.Length - 1) + "ies";
            }
            else if (name.EndsWith("s") == false)
            {
                return name + "s";
            }

            return name;
        }

        public static string Singular(string name)
        {
            if (name.EndsWith("es", StringComparison.CurrentCultureIgnoreCase))
            {
                var rest = name.Substring(0, name.Length - 2);

                if (rest.EndsWith("x", StringComparison.CurrentCultureIgnoreCase) || name.EndsWith("ch", StringComparison.CurrentCultureIgnoreCase) || name.EndsWith("ss", StringComparison.CurrentCultureIgnoreCase))
                {
                    return rest;
                }
            }

            if (name.EndsWith("ies", StringComparison.CurrentCultureIgnoreCase))
            {
                return name.Substring(0, name.Length - 3) + "y";
            }
            else if (name.EndsWith("s", StringComparison.CurrentCultureIgnoreCase) && !name.EndsWith("ss", StringComparison.CurrentCultureIgnoreCase))
            {
                return name.Substring(0, name.Length - 1);
            }

            return name;
        }
    }
}