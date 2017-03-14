using System;
using System.Collections;
using System.Collections.Generic;

namespace IQToolkit
{
    internal class CompoundKey : IEquatable<CompoundKey>, IEnumerable<object>
    {
        private readonly object[] values;
        private readonly int hc;

        public CompoundKey(params object[] values)
        {
            this.values = values;

            for (int i = 0, n = values.Length; i < n; i++)
            {
                var value = values[i];

                if (value != null)
                {
                    hc ^= (value.GetHashCode() + i);
                }
            }
        }

        public IEnumerator<object> GetEnumerator()
        {
            return (values as IEnumerable<object>).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public bool Equals(CompoundKey other)
        {
            if (other == null || other.values.Length != values.Length)
            {
                return false;
            }

            for (int i = 0, n = other.values.Length; i < n; i++)
            {
                if (object.Equals(this.values[i], other.values[i]) == false)
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return hc;
        }
    }
}