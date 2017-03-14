using IQToolkit;
using IQToolkit.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;

namespace Job.Framework.DataAccess.Tests.IQToolkit
{
    [TestClass]
    public class NorthwindCUD
    {
        private readonly Northwind db;

        public NorthwindCUD()
        {
            db = new Northwind();

            //ExecSilent("DELETE FROM Orders WHERE CustomerID LIKE 'XX%'");
            //ExecSilent("DELETE FROM Customers WHERE CustomerID LIKE 'XX%'");
        }

        private int ExecSilent(string commandText)
        {
            return this.db.ExecuteNonQueryAsync(commandText).Result;
        }

        [TestMethod]
        public void TestInsertCustomerNoResult()
        {
            var cust = new Customer
            {
                CustomerID = "XX1",
                CompanyName = "Company1",
                ContactName = "Contact1",
                City = "Seattle",
                Country = "USA"
            };
            var result = db.Customers.Insert(cust);
            Assert.AreEqual(1, result);  // returns 1 for success
        }

        [TestMethod]
        public void TestInsertCustomerWithResult()
        {
            var cust = new Customer
            {
                CustomerID = "XX1",
                CompanyName = "Company1",
                ContactName = "Contact1",
                City = "Seattle",
                Country = "USA"
            };
            var result = db.Customers.Insert(cust, c => c.City);
            Assert.AreEqual(result, "Seattle");  // should be value we asked for
        }

        [TestMethod]
        public void TestBatchInsertCustomersNoResult()
        {
            int n = 10;
            var custs = Enumerable.Range(1, n).Select(
                i => new Customer
                {
                    CustomerID = "XX" + i,
                    CompanyName = "Company" + i,
                    ContactName = "Contact" + i,
                    City = "Seattle",
                    Country = "USA"
                });
            var results = db.Customers.Batch(custs, (u, c) => u.Insert(c));
            Assert.AreEqual(n, results.Count());
            Assert.AreEqual(true, results.All(r => object.Equals(r, 1)));
        }

        [TestMethod]
        public void TestBatchInsertCustomersWithResult()
        {
            int n = 10;
            var custs = Enumerable.Range(1, n).Select(
                i => new Customer
                {
                    CustomerID = "XX" + i,
                    CompanyName = "Company" + i,
                    ContactName = "Contact" + i,
                    City = "Seattle",
                    Country = "USA"
                });
            var results = db.Customers.Batch(custs, (u, c) => u.Insert(c, d => d.City));
            Assert.AreEqual(n, results.Count());
            Assert.AreEqual(true, results.All(r => object.Equals(r, "Seattle")));
        }

        [TestMethod]
        public void TestInsertOrderWithNoResult()
        {
            this.TestInsertCustomerNoResult(); // create customer "XX1"
            var order = new Order
            {
                CustomerID = "XX1",
                OrderDate = DateTime.Today,
            };
            var result = db.Orders.Insert(order);
            Assert.AreEqual(1, result);
        }

        [TestMethod]
        public void TestInsertOrderWithGeneratedIDResult()
        {
            this.TestInsertCustomerNoResult(); // create customer "XX1"
            var order = new Order
            {
                CustomerID = "XX1",
                OrderDate = DateTime.Today,
            };
            var result = db.Orders.Insert(order, o => o.OrderID);
            Assert.AreNotEqual(1, result);
        }

        [TestMethod]
        public void TestUpdateCustomerNoResult()
        {
            this.TestInsertCustomerNoResult(); // create customer "XX1"

            var cust = new Customer
            {
                CustomerID = "XX1",
                CompanyName = "Company1",
                ContactName = "Contact1",
                City = "Portland", // moved to Portland!
                Country = "USA"
            };

            var result = db.Customers.Update(cust);
            Assert.AreEqual(1, result);
        }

        [TestMethod]
        public void TestUpdateCustomerWithResult()
        {
            this.TestInsertCustomerNoResult(); // create customer "XX1"

            var cust = new Customer
            {
                CustomerID = "XX1",
                CompanyName = "Company1",
                ContactName = "Contact1",
                City = "Portland", // moved to Portland!
                Country = "USA"
            };

            var result = db.Customers.Update(cust, null, c => c.City);
            Assert.AreEqual("Portland", result);
        }

        [TestMethod]
        public void TestUpdateCustomerWithUpdateCheckThatDoesNotSucceed()
        {
            this.TestInsertCustomerNoResult(); // create customer "XX1"

            var cust = new Customer
            {
                CustomerID = "XX1",
                CompanyName = "Company1",
                ContactName = "Contact1",
                City = "Portland", // moved to Portland!
                Country = "USA"
            };

            var result = db.Customers.Update(cust, d => d.City == "Detroit");
            Assert.AreEqual(0, result); // 0 for failure
        }

        [TestMethod]
        public void TestUpdateCustomerWithUpdateCheckThatSucceeds()
        {
            this.TestInsertCustomerNoResult(); // create customer "XX1"

            var cust = new Customer
            {
                CustomerID = "XX1",
                CompanyName = "Company1",
                ContactName = "Contact1",
                City = "Portland", // moved to Portland!
                Country = "USA"
            };

            var result = db.Customers.Update(cust, d => d.City == "Seattle");
            Assert.AreEqual(1, result);
        }

        [TestMethod]
        public void TestBatchUpdateCustomer()
        {
            this.TestBatchInsertCustomersNoResult();

            int n = 10;
            var custs = Enumerable.Range(1, n).Select(
                i => new Customer
                {
                    CustomerID = "XX" + i,
                    CompanyName = "Company" + i,
                    ContactName = "Contact" + i,
                    City = "Seattle",
                    Country = "USA"
                });

            var results = db.Customers.Batch(custs, (u, c) => u.Update(c));
            Assert.AreEqual(n, results.Count());
            Assert.AreEqual(true, results.All(r => object.Equals(r, 1)));
        }

        [TestMethod]
        public void TestBatchUpdateCustomerWithUpdateCheck()
        {
            this.TestBatchInsertCustomersNoResult();

            int n = 10;
            var pairs = Enumerable.Range(1, n).Select(
                i => new
                {
                    original = new Customer
                    {
                        CustomerID = "XX" + i,
                        CompanyName = "Company" + i,
                        ContactName = "Contact" + i,
                        City = "Seattle",
                        Country = "USA"
                    },
                    current = new Customer
                    {
                        CustomerID = "XX" + i,
                        CompanyName = "Company" + i,
                        ContactName = "Contact" + i,
                        City = "Portland",
                        Country = "USA"
                    }
                });

            var results = db.Customers.Batch(pairs, (u, x) => u.Update(x.current, d => d.City == x.original.City));
            Assert.AreEqual(n, results.Count());
            Assert.AreEqual(true, results.All(r => object.Equals(r, 1)));
        }

        [TestMethod]
        public void TestBatchUpdateCustomerWithResult()
        {
            this.TestBatchInsertCustomersNoResult();

            int n = 10;
            var custs = Enumerable.Range(1, n).Select(
                i => new Customer
                {
                    CustomerID = "XX" + i,
                    CompanyName = "Company" + i,
                    ContactName = "Contact" + i,
                    City = "Portland",
                    Country = "USA"
                });

            var results = db.Customers.Batch(custs, (u, c) => u.Update(c, null, d => d.City));
            Assert.AreEqual(n, results.Count());
            Assert.AreEqual(true, results.All(r => object.Equals(r, "Portland")));
        }

        [TestMethod]
        public void TestBatchUpdateCustomerWithUpdateCheckAndResult()
        {
            this.TestBatchInsertCustomersNoResult();

            int n = 10;
            var pairs = Enumerable.Range(1, n).Select(
                i => new
                {
                    original = new Customer
                    {
                        CustomerID = "XX" + i,
                        CompanyName = "Company" + i,
                        ContactName = "Contact" + i,
                        City = "Seattle",
                        Country = "USA"
                    },
                    current = new Customer
                    {
                        CustomerID = "XX" + i,
                        CompanyName = "Company" + i,
                        ContactName = "Contact" + i,
                        City = "Portland",
                        Country = "USA"
                    }
                });

            var results = db.Customers.Batch(pairs, (u, x) => u.Update(x.current, d => d.City == x.original.City, d => d.City));
            Assert.AreEqual(n, results.Count());
            Assert.AreEqual(true, results.All(r => object.Equals(r, "Portland")));
        }

        [TestMethod]
        public void TestUpsertNewCustomerNoResult()
        {
            var cust = new Customer
            {
                CustomerID = "XX1",
                CompanyName = "Company1",
                ContactName = "Contact1",
                City = "Seattle", // moved to Portland!
                Country = "USA"
            };

            var result = db.Customers.InsertOrUpdate(cust);
            Assert.AreEqual(1, result);
        }

        [TestMethod]
        public void TestUpsertExistingCustomerNoResult()
        {
            this.TestInsertCustomerNoResult();

            var cust = new Customer
            {
                CustomerID = "XX1",
                CompanyName = "Company1",
                ContactName = "Contact1",
                City = "Portland", // moved to Portland!
                Country = "USA"
            };

            var result = db.Customers.InsertOrUpdate(cust);
            Assert.AreEqual(1, result);
        }

        [TestMethod]
        public void TestUpsertNewCustomerWithResult()
        {
            var cust = new Customer
            {
                CustomerID = "XX1",
                CompanyName = "Company1",
                ContactName = "Contact1",
                City = "Seattle", // moved to Portland!
                Country = "USA"
            };

            var result = db.Customers.InsertOrUpdate(cust, null, d => d.City);
            Assert.AreEqual("Seattle", result);
        }

        [TestMethod]
        public void TestUpsertExistingCustomerWithResult()
        {
            this.TestInsertCustomerNoResult();

            var cust = new Customer
            {
                CustomerID = "XX1",
                CompanyName = "Company1",
                ContactName = "Contact1",
                City = "Portland", // moved to Portland!
                Country = "USA"
            };

            var result = db.Customers.InsertOrUpdate(cust, null, d => d.City);
            Assert.AreEqual("Portland", result);
        }

        [TestMethod]
        public void TestUpsertNewCustomerWithUpdateCheck()
        {
            var cust = new Customer
            {
                CustomerID = "XX1",
                CompanyName = "Company1",
                ContactName = "Contact1",
                City = "Portland", // moved to Portland!
                Country = "USA"
            };

            var result = db.Customers.InsertOrUpdate(cust, d => d.City == "Portland");
            Assert.AreEqual(1, result);
        }

        [TestMethod]
        public void TestUpsertExistingCustomerWithUpdateCheck()
        {
            this.TestInsertCustomerNoResult();

            var cust = new Customer
            {
                CustomerID = "XX1",
                CompanyName = "Company1",
                ContactName = "Contact1",
                City = "Portland", // moved to Portland!
                Country = "USA"
            };

            var result = db.Customers.InsertOrUpdate(cust, d => d.City == "Seattle");
            Assert.AreEqual(1, result);
        }

        [TestMethod]
        public void TestBatchUpsertNewCustomersNoResult()
        {
            int n = 10;
            var custs = Enumerable.Range(1, n).Select(
                i => new Customer
                {
                    CustomerID = "XX" + i,
                    CompanyName = "Company" + i,
                    ContactName = "Contact" + i,
                    City = "Portland",
                    Country = "USA"
                });

            var results = db.Customers.Batch(custs, (u, c) => u.InsertOrUpdate(c));
            Assert.AreEqual(n, results.Count());
            Assert.AreEqual(true, results.All(r => object.Equals(r, 1)));
        }

        [TestMethod]
        public void TestBatchUpsertExistingCustomersNoResult()
        {
            this.TestBatchInsertCustomersNoResult();

            int n = 10;
            var custs = Enumerable.Range(1, n).Select(
                i => new Customer
                {
                    CustomerID = "XX" + i,
                    CompanyName = "Company" + i,
                    ContactName = "Contact" + i,
                    City = "Portland",
                    Country = "USA"
                });

            var results = db.Customers.Batch(custs, (u, c) => u.InsertOrUpdate(c));
            Assert.AreEqual(n, results.Count());
            Assert.AreEqual(true, results.All(r => object.Equals(r, 1)));
        }

        [TestMethod]
        public void TestBatchUpsertNewCustomersWithResult()
        {
            int n = 10;
            var custs = Enumerable.Range(1, n).Select(
                i => new Customer
                {
                    CustomerID = "XX" + i,
                    CompanyName = "Company" + i,
                    ContactName = "Contact" + i,
                    City = "Portland",
                    Country = "USA"
                });

            var results = db.Customers.Batch(custs, (u, c) => u.InsertOrUpdate(c, null, d => d.City));
            Assert.AreEqual(n, results.Count());
            Assert.AreEqual(true, results.All(r => object.Equals(r, "Portland")));
        }

        [TestMethod]
        public void TestBatchUpsertExistingCustomersWithResult()
        {
            this.TestBatchInsertCustomersNoResult();

            int n = 10;
            var custs = Enumerable.Range(1, n).Select(
                i => new Customer
                {
                    CustomerID = "XX" + i,
                    CompanyName = "Company" + i,
                    ContactName = "Contact" + i,
                    City = "Portland",
                    Country = "USA"
                });

            var results = db.Customers.Batch(custs, (u, c) => u.InsertOrUpdate(c, null, d => d.City));
            Assert.AreEqual(n, results.Count());
            Assert.AreEqual(true, results.All(r => object.Equals(r, "Portland")));
        }

        [TestMethod]
        public void TestBatchUpsertNewCustomersWithUpdateCheck()
        {
            int n = 10;
            var pairs = Enumerable.Range(1, n).Select(
                i => new
                {
                    original = new Customer
                    {
                        CustomerID = "XX" + i,
                        CompanyName = "Company" + i,
                        ContactName = "Contact" + i,
                        City = "Seattle",
                        Country = "USA"
                    },
                    current = new Customer
                    {
                        CustomerID = "XX" + i,
                        CompanyName = "Company" + i,
                        ContactName = "Contact" + i,
                        City = "Portland",
                        Country = "USA"
                    }
                });

            var results = db.Customers.Batch(pairs, (u, x) => u.InsertOrUpdate(x.current, d => d.City == x.original.City));
            Assert.AreEqual(n, results.Count());
            Assert.AreEqual(true, results.All(r => object.Equals(r, 1)));
        }

        [TestMethod]
        public void TestBatchUpsertExistingCustomersWithUpdateCheck()
        {
            this.TestBatchInsertCustomersNoResult();

            int n = 10;
            var pairs = Enumerable.Range(1, n).Select(
                i => new
                {
                    original = new Customer
                    {
                        CustomerID = "XX" + i,
                        CompanyName = "Company" + i,
                        ContactName = "Contact" + i,
                        City = "Seattle",
                        Country = "USA"
                    },
                    current = new Customer
                    {
                        CustomerID = "XX" + i,
                        CompanyName = "Company" + i,
                        ContactName = "Contact" + i,
                        City = "Portland",
                        Country = "USA"
                    }
                });

            var results = db.Customers.Batch(pairs, (u, x) => u.InsertOrUpdate(x.current, d => d.City == x.original.City));
            Assert.AreEqual(n, results.Count());
            Assert.AreEqual(true, results.All(r => object.Equals(r, 1)));
        }

        [TestMethod]
        public void TestDeleteCustomer()
        {
            this.TestInsertCustomerNoResult();

            var cust = new Customer
            {
                CustomerID = "XX1",
                CompanyName = "Company1",
                ContactName = "Contact1",
                City = "Seattle",
                Country = "USA"
            };

            var result = db.Customers.Delete(cust);
            Assert.AreEqual(1, result);
        }

        [TestMethod]
        public void TestDeleteCustomerForNonExistingCustomer()
        {
            this.TestInsertCustomerNoResult();

            var cust = new Customer
            {
                CustomerID = "XX2",
                CompanyName = "Company2",
                ContactName = "Contact2",
                City = "Seattle",
                Country = "USA"
            };

            var result = db.Customers.Delete(cust);
            Assert.AreEqual(0, result);
        }

        [TestMethod]
        public void TestDeleteCustomerWithDeleteCheckThatSucceeds()
        {
            this.TestInsertCustomerNoResult();

            var cust = new Customer
            {
                CustomerID = "XX1",
                CompanyName = "Company1",
                ContactName = "Contact1",
                City = "Seattle",
                Country = "USA"
            };

            var result = db.Customers.Delete(cust, d => d.City == "Seattle");
            Assert.AreEqual(1, result);
        }

        [TestMethod]
        public void TestDeleteCustomerWithDeleteCheckThatDoesNotSucceed()
        {
            this.TestInsertCustomerNoResult();

            var cust = new Customer
            {
                CustomerID = "XX1",
                CompanyName = "Company1",
                ContactName = "Contact1",
                City = "Seattle",
                Country = "USA"
            };

            var result = db.Customers.Delete(cust, d => d.City == "Portland");
            Assert.AreEqual(0, result);
        }

        [TestMethod]
        public void TestBatchDeleteCustomers()
        {
            this.TestBatchInsertCustomersNoResult();

            int n = 10;
            var custs = Enumerable.Range(1, n).Select(
                i => new Customer
                {
                    CustomerID = "XX" + i,
                    CompanyName = "Company" + i,
                    ContactName = "Contact" + i,
                    City = "Seattle",
                    Country = "USA"
                });

            var results = db.Customers.Batch(custs, (u, c) => u.Delete(c));
            Assert.AreEqual(n, results.Count());
            Assert.AreEqual(true, results.All(r => object.Equals(r, 1)));
        }

        [TestMethod]
        public void TestBatchDeleteCustomersWithDeleteCheck()
        {
            this.TestBatchInsertCustomersNoResult();

            int n = 10;
            var custs = Enumerable.Range(1, n).Select(
                i => new Customer
                {
                    CustomerID = "XX" + i,
                    CompanyName = "Company" + i,
                    ContactName = "Contact" + i,
                    City = "Seattle",
                    Country = "USA"
                });

            var results = db.Customers.Batch(custs, (u, c) => u.Delete(c, d => d.City == c.City));
            Assert.AreEqual(n, results.Count());
            Assert.AreEqual(true, results.All(r => object.Equals(r, 1)));
        }

        [TestMethod]
        public void TestBatchDeleteCustomersWithDeleteCheckThatDoesNotSucceed()
        {
            this.TestBatchInsertCustomersNoResult();

            int n = 10;
            var custs = Enumerable.Range(1, n).Select(
                i => new Customer
                {
                    CustomerID = "XX" + i,
                    CompanyName = "Company" + i,
                    ContactName = "Contact" + i,
                    City = "Portland",
                    Country = "USA"
                });

            var results = db.Customers.Batch(custs, (u, c) => u.Delete(c, d => d.City == c.City));
            Assert.AreEqual(n, results.Count());
            Assert.AreEqual(true, results.All(r => object.Equals(r, 0)));
        }

        [TestMethod]
        public void TestDeleteWhere()
        {
            this.TestBatchInsertCustomersNoResult();

            var result = db.Customers.Delete(c => c.CustomerID.StartsWith("XX"));
            Assert.AreEqual(10, result);
        }
    }
}
