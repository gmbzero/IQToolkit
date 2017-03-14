using IQToolkit;
using IQToolkit.Data;
using IQToolkit.Data.Mapping;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Job.Framework.DataAccess.Tests.IQToolkit
{
    public class Customer
    {
        public string CustomerID;
        public string ContactName;
        public string CompanyName;
        public string Phone;
        public string City;
        public string Country;
        public IList<Order> Orders;
    }

    public class Order
    {
        public int OrderID;
        public string CustomerID;
        public DateTime OrderDate;
        public Customer Customer;
        public List<OrderDetail> Details;
    }

    public class OrderDetail
    {
        public int? OrderID { get; set; }
        public int ProductID { get; set; }
        public Product Product;
    }

    public interface IEntity
    {
        int ID { get; }
    }

    public class Product : IEntity
    {
        public int ID;
        public string ProductName;
        public bool Discontinued;

        int IEntity.ID
        {
            get { return this.ID; }
        }
    }

    public class Employee
    {
        public int EmployeeID;
        public string LastName;
        public string FirstName;
        public string Title;
        public Address Address;
    }

    public class Address
    {
        public string Street { get; private set; }
        public string City { get; private set; }
        public string Region { get; private set; }
        public string PostalCode { get; private set; }

        public Address(string street, string city, string region, string postalCode)
        {
            this.Street = street;
            this.City = city;
            this.Region = region;
            this.PostalCode = postalCode;
        }
    }

    public class NorthwindSession : DbSession
    {
        public ISession<Customer> Customers { get; set; }

        public ISession<Order> Orders { get; set; }

        public ISession<OrderDetail> OrderDetails { get; set; }

        public ISession<Product> Products { get; set; }

        public ISession<Employee> Employees { get; set; }

        protected override void OnConfiguring(DbContextOptins options)
        {
            options.UseDbConnection(new SqlConnection
            {
                ConnectionString = "Data Source=.;Initial Catalog=Northwind;MultipleActiveResultSets=true;User Id=sa;Password=138jobcom;"
            });
        }
    }

    public class Northwind : DbEntity
    {
        public virtual IEntity<Customer> Customers { get; set; }

        public virtual IEntity<Order> Orders { get; set; }

        public virtual IEntity<OrderDetail> OrderDetails { get; set; }

        public virtual IEntity<Product> Products { get; set; }

        public virtual IEntity<Employee> Employees { get; set; }

        protected override void OnConfiguring(DbContextOptins options)
        {
            options.UseDbConnection(new SqlConnection
            {
                ConnectionString = "Data Source=.;Initial Catalog=Northwind;MultipleActiveResultSets=true;User Id=sa;Password=138jobcom;"
            });
        }
    }

    public class NorthwindWithAttributes : Northwind
    {
        [Table]
        [Column(Member = "CustomerId", IsPrimaryKey = true)]
        [Column(Member = "ContactName")]
        [Column(Member = "CompanyName")]
        [Column(Member = "Phone")]
        [Column(Member = "City", DbType = "NVARCHAR(20)")]
        [Column(Member = "Country")]
        [Association(Member = "Orders", KeyMembers = "CustomerID", RelatedEntityID = "Orders", RelatedKeyMembers = "CustomerID")]
        public override IEntity<Customer> Customers { get; set; }

        [Table]
        [Column(Member = "OrderID", IsPrimaryKey = true, IsGenerated = true)]
        [Column(Member = "CustomerID")]
        [Column(Member = "OrderDate")]
        [Association(Member = "Customer", KeyMembers = "CustomerID", RelatedEntityID = "Customers", RelatedKeyMembers = "CustomerID")]
        [Association(Member = "Details", KeyMembers = "OrderID", RelatedEntityID = "OrderDetails", RelatedKeyMembers = "OrderID")]
        public override IEntity<Order> Orders { get; set; }

        [Table(Name = "Order Details")]
        [Column(Member = "OrderID", IsPrimaryKey = true)]
        [Column(Member = "ProductID", IsPrimaryKey = true)]
        [Association(Member = "Product", KeyMembers = "ProductID", RelatedEntityID = "Products", RelatedKeyMembers = "ID")]
        public override IEntity<OrderDetail> OrderDetails { get; set; }

        [Table]
        [Column(Member = "Id", Name = "ProductId", IsPrimaryKey = true)]
        [Column(Member = "ProductName")]
        [Column(Member = "Discontinued")]
        public override IEntity<Product> Products { get; set; }

        [Table]
        [Column(Member = "EmployeeID", IsPrimaryKey = true)]
        [Column(Member = "LastName")]
        [Column(Member = "FirstName")]
        [Column(Member = "Title")]
        [Column(Member = "Address.Street", Name = "Address")]
        [Column(Member = "Address.City")]
        [Column(Member = "Address.Region")]
        [Column(Member = "Address.PostalCode")]
        public override IEntity<Employee> Employees { get; set; }
    }
}