//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using System;
//using System.Linq;
//using System.Collections.Generic;
//using System.Text;
//using IQToolkit;

//namespace Job.Framework.DataAccess.Tests.IQToolkit
//{
//    [TestClass]
//    public class NorthwindCUDSession
//    {
//        private readonly NorthwindSession db;

//        public NorthwindCUDSession()
//        {
//            db = new NorthwindSession();

//            ExecSilent("DELETE FROM Orders WHERE CustomerID LIKE 'XX%'");
//            ExecSilent("DELETE FROM Customers WHERE CustomerID LIKE 'XX%'");
//        }

//        private int ExecSilent(string commandText)
//        {
//            return this.db.ExecuteNonQueryAsync(commandText).Result;
//        }
        
//        public void TestSessionIdentityCache()
//        {
//            // both objects should be the same instance
//            var cust = db.Customers.Single(c => c.CustomerID == "ALFKI");
//            var cust2 = db.Customers.Single(c => c.CustomerID == "ALFKI");

//            Assert.AreNotEqual(null, cust);
//            Assert.AreNotEqual(null, cust2);
//            Assert.AreSame(cust, cust2);
//        }

//        public void TestSessionProviderNotIdentityCached()
//        {
//            // both objects should be different instances
//            var cust = db.Customers.Single(c => c.CustomerID == "ALFKI");
//            var cust2 = db.Customers.ProviderTable.Single(c => c.CustomerID == "ALFKI");

//            Assert.AreNotEqual(null, cust);
//            Assert.AreNotEqual(null, cust2);
//            Assert.AreEqual(cust.CustomerID, cust2.CustomerID);
//            Assert.AreNotSame(cust, cust2);
//        }

//        public void TestSessionSubmitActionOnModify()
//        {
//            var cust = new Customer
//            {
//                CustomerID = "XX1",
//                CompanyName = "Company1",
//                ContactName = "Contact1",
//                City = "Seattle",
//                Country = "USA"
//            };

//            this.db.Customers.Insert(cust);

//            var ns = new NorthwindSession(this.GetProvider());
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));

//            // fetch the previously inserted customer
//            cust = db.Customers.Single(c => c.CustomerID == "XX1");
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));

//            cust.ContactName = "Contact Modified";
//            Assert.AreEqual(SubmitAction.Update, db.Customers.GetSubmitAction(cust));

//            db.SubmitChanges();
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));

//            // prove actually modified by fetching through provider
//            var cust2 = this.db.Customers.Single(c => c.CustomerID == "XX1");
//            Assert.AreEqual("Contact Modified", cust2.ContactName);

//            // ready to be submitted again!
//            cust.City = "SeattleX";
//            Assert.AreEqual(SubmitAction.Update, db.Customers.GetSubmitAction(cust));
//        }

//        public void TestSessionSubmitActionOnInsert()
//        {
//            NorthwindSession ns = new NorthwindSession(this.GetProvider());
//            var cust = new Customer
//            {
//                CustomerID = "XX1",
//                CompanyName = "Company1",
//                ContactName = "Contact1",
//                City = "Seattle",
//                Country = "USA"
//            };
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));

//            db.Customers.InsertOnSubmit(cust);
//            Assert.AreEqual(SubmitAction.Insert, db.Customers.GetSubmitAction(cust));

//            db.SubmitChanges();
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));

//            cust.City = "SeattleX";
//            Assert.AreEqual(SubmitAction.Update, db.Customers.GetSubmitAction(cust));
//        }

//        public void TestSessionSubmitActionOnInsertOrUpdate()
//        {
//            NorthwindSession ns = new NorthwindSession(this.GetProvider());
//            var cust = new Customer
//            {
//                CustomerID = "XX1",
//                CompanyName = "Company1",
//                ContactName = "Contact1",
//                City = "Seattle",
//                Country = "USA"
//            };
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));

//            db.Customers.InsertOrUpdateOnSubmit(cust);
//            Assert.AreEqual(SubmitAction.InsertOrUpdate, db.Customers.GetSubmitAction(cust));

//            db.SubmitChanges();
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));

//            cust.City = "SeattleX";
//            Assert.AreEqual(SubmitAction.Update, db.Customers.GetSubmitAction(cust));
//        }

//        public void TestSessionSubmitActionOnUpdate()
//        {
//            var cust = new Customer
//            {
//                CustomerID = "XX1",
//                CompanyName = "Company1",
//                ContactName = "Contact1",
//                City = "Seattle",
//                Country = "USA"
//            };
//            this.db.Customers.Insert(cust);

//            NorthwindSession ns = new NorthwindSession(this.GetProvider());
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));

//            db.Customers.UpdateOnSubmit(cust);
//            Assert.AreEqual(SubmitAction.Update, db.Customers.GetSubmitAction(cust));

//            db.SubmitChanges();
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));

//            cust.City = "SeattleX";
//            Assert.AreEqual(SubmitAction.Update, db.Customers.GetSubmitAction(cust));
//        }

//        public void TestSessionSubmitActionOnDelete()
//        {
//            var cust = new Customer
//            {
//                CustomerID = "XX1",
//                CompanyName = "Company1",
//                ContactName = "Contact1",
//                City = "Seattle",
//                Country = "USA"
//            };
//            this.db.Customers.Insert(cust);

//            NorthwindSession ns = new NorthwindSession(this.GetProvider());
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));

//            db.Customers.DeleteOnSubmit(cust);
//            Assert.AreEqual(SubmitAction.Delete, db.Customers.GetSubmitAction(cust));

//            db.SubmitChanges();
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));

//            // modifications after delete don't trigger updates
//            cust.City = "SeattleX";
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));
//        }

//        public void TestDeleteThenInsertSamePK()
//        {
//            var cust = new Customer
//            {
//                CustomerID = "XX1",
//                CompanyName = "Company1",
//                ContactName = "Contact1",
//                City = "Seattle",
//                Country = "USA"
//            };

//            var cust2 = new Customer
//            {
//                CustomerID = "XX1",
//                CompanyName = "Company2",
//                ContactName = "Contact2",
//                City = "Chicago",
//                Country = "USA"
//            };

//            this.db.Customers.Insert(cust);

//            NorthwindSession ns = new NorthwindSession(this.GetProvider());
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust2));

//            db.Customers.DeleteOnSubmit(cust);
//            Assert.AreEqual(SubmitAction.Delete, db.Customers.GetSubmitAction(cust));
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust2));

//            db.Customers.InsertOnSubmit(cust2);
//            Assert.AreEqual(SubmitAction.Delete, db.Customers.GetSubmitAction(cust));
//            Assert.AreEqual(SubmitAction.Insert, db.Customers.GetSubmitAction(cust2));

//            db.SubmitChanges();
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust2));

//            // modifications after delete don't trigger updates
//            cust.City = "SeattleX";
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));

//            // modifications after insert do trigger updates
//            cust2.City = "ChicagoX";
//            Assert.AreEqual(SubmitAction.Update, db.Customers.GetSubmitAction(cust2));
//        }

//        public void TestInsertThenDeleteSamePK()
//        {
//            var cust = new Customer
//            {
//                CustomerID = "XX1",
//                CompanyName = "Company1",
//                ContactName = "Contact1",
//                City = "Seattle",
//                Country = "USA"
//            };

//            var cust2 = new Customer
//            {
//                CustomerID = "XX1",
//                CompanyName = "Company2",
//                ContactName = "Contact2",
//                City = "Chicago",
//                Country = "USA"
//            };

//            this.db.Customers.Insert(cust);

//            NorthwindSession ns = new NorthwindSession(this.GetProvider());
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust2));

//            db.Customers.InsertOnSubmit(cust2);
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));
//            Assert.AreEqual(SubmitAction.Insert, db.Customers.GetSubmitAction(cust2));

//            db.Customers.DeleteOnSubmit(cust);
//            Assert.AreEqual(SubmitAction.Delete, db.Customers.GetSubmitAction(cust));
//            Assert.AreEqual(SubmitAction.Insert, db.Customers.GetSubmitAction(cust2));

//            db.SubmitChanges();
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust2));

//            // modifications after delete don't trigger updates
//            cust.City = "SeattleX";
//            Assert.AreEqual(SubmitAction.None, db.Customers.GetSubmitAction(cust));

//            // modifications after insert do trigger updates
//            cust2.City = "ChicagoX";
//            Assert.AreEqual(SubmitAction.Update, db.Customers.GetSubmitAction(cust2));
//        }
//    }
//}
