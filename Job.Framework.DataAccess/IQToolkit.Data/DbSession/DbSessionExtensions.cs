using System;
using System.Collections.Generic;
using System.Text;

namespace IQToolkit.Data
{
    public static class DbSessionExtensions
    {
        public static void InsertOnSubmit<T>(this ISession<T> table, T instance)
        {
            table.SetSubmitAction(instance, SubmitAction.Insert);
        }

        public static void InsertOnSubmit(this ISession table, object instance)
        {
            table.SetSubmitAction(instance, SubmitAction.Insert);
        }

        public static void InsertOrUpdateOnSubmit<T>(this ISession<T> table, T instance)
        {
            table.SetSubmitAction(instance, SubmitAction.InsertOrUpdate);
        }

        public static void InsertOrUpdateOnSubmit(this ISession table, object instance)
        {
            table.SetSubmitAction(instance, SubmitAction.InsertOrUpdate);
        }

        public static void UpdateOnSubmit<T>(this ISession<T> table, T instance)
        {
            table.SetSubmitAction(instance, SubmitAction.Update);
        }

        public static void UpdateOnSubmit(this ISession table, object instance)
        {
            table.SetSubmitAction(instance, SubmitAction.Update);
        }

        public static void DeleteOnSubmit<T>(this ISession<T> table, T instance)
        {
            table.SetSubmitAction(instance, SubmitAction.Delete);
        }

        public static void DeleteOnSubmit(this ISession table, object instance)
        {
            table.SetSubmitAction(instance, SubmitAction.Delete);
        }
    }
}
