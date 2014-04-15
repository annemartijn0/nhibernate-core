using System;
using System.Collections;
using NHibernate.Dialect;
using NUnit.Framework;

namespace NHibernate.Test.Hql
{
	/// <summary>
	/// This test run each HQL function separatelly so is easy to know wich function need
	/// an override in the specific dialect implementation.
	/// </summary>
	[TestFixture]
	public class HQLFunctionsAsync : TestCase
	{
		static readonly Hashtable notSupportedStandardFunction;
		static HQLFunctionsAsync()
		{
			notSupportedStandardFunction =
				new Hashtable
					{
						{"locate", new[] {typeof (SQLiteDialect)}},
						{"bit_length", new[] {typeof (SQLiteDialect)}},
						{"extract", new[] {typeof (SQLiteDialect)}},
						{"nullif", new[] {typeof (Oracle8iDialect)}}
					};
		}

		private bool IsOracleDialect()
		{
			return Dialect is Oracle8iDialect;
		}

		private void IgnoreIfNotSupported(string functionName)
		{
			if (notSupportedStandardFunction.ContainsKey(functionName))
			{
				IList dialects = (IList)notSupportedStandardFunction[functionName];
				if(dialects.Contains(Dialect.GetType()))
					Assert.Ignore(Dialect + " doesn't support "+functionName+" function.");
			}
		}

		protected override string MappingsAssembly
		{
			get { return "NHibernate.Test"; }
		}

		protected override IList Mappings
		{
			get { return new string[] { "Hql.Animal.hbm.xml", "Hql.MaterialResource.hbm.xml" }; }
		}

		protected override void OnTearDown()
		{
			using (ISession s = OpenSession())
			{
				s.Delete("from Human");
				s.Delete("from Animal");
				s.Flush();
			}
		}

		[Test]
		public void AggregateCountAsync()
		{
			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("a1", 20);
				Animal a2 = new Animal("a2", 10);
				s.Save(a1);
				s.Save(a2);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				object result = null;
				// Act
				// Count in select
				s.CreateQuery("select count(distinct a.id) from Animal a").UniqueResultAsync()
					.ContinueWith(task =>
					{
						result = task.Result;

						// Assert
						Assert.That(result.GetType(), Is.InstanceOf(typeof(long)));
						Assert.That(result, Is.EqualTo(2));
					}).Wait();
				
				// Act
				s.CreateQuery("select count(*) from Animal").UniqueResultAsync()
					.ContinueWith(task =>
					{
						result = task.Result;

						// Assert
						Assert.That(result.GetType(), Is.InstanceOf(typeof(long)));
						Assert.That(result, Is.EqualTo(2));
					}).Wait();
				

				// Count in where
				if (TestDialect.SupportsHavingWithoutGroupBy)
				{
					// Act
					s.CreateQuery("select count(a.id) from Animal a having count(a.id)>1").UniqueResultAsync()
						.ContinueWith(task =>
						{
							result = task.Result;

							// Assert
							Assert.That(result.GetType(), Is.InstanceOf(typeof (long)));
							Assert.That(result, Is.EqualTo(2));
						}).Wait();
				}
			}
		}

		[Test]
		public void AggregateAvgAsync()
		{
			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("a1", 20);
				Animal a2 = new Animal("a2", 10);
				s.Save(a1);
				s.Save(a2);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				object result = null;
				// Act
				// In Select
				s.CreateQuery("select avg(a.BodyWeight) from Animal a").UniqueResultAsync()
					.ContinueWith(task =>
					{
						result = task.Result;

						// Assert
						Assert.That(result.GetType(), Is.InstanceOf(typeof(double)));
						Assert.That(result, Is.EqualTo(15D));
					}).Wait();
				

				// In where
				if (TestDialect.SupportsHavingWithoutGroupBy)
				{
					// Act
					s.CreateQuery("select avg(a.BodyWeight) from Animal a having avg(a.BodyWeight)>0").UniqueResultAsync()
						.ContinueWith(task =>
						{
							result = task.Result;

							// Assert
							Assert.That(result.GetType(), Is.InstanceOf(typeof(double)));
							Assert.That(result, Is.EqualTo(15D));
						}).Wait();
				}
			}
		}

		[Test]
		public void AggregateMaxAsync()
		{
			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("a1", 20);
				Animal a2 = new Animal("a2", 10);
				s.Save(a1);
				s.Save(a2);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				object result = null;
				// Act
				s.CreateQuery("select max(a.BodyWeight) from Animal a").UniqueResultAsync()
					.ContinueWith(task =>
					{
						result = task.Result;

						// Assert
						Assert.That(result.GetType(), Is.InstanceOf(typeof(float)));
						Assert.That(result, Is.EqualTo(20F));
					}).Wait();

				if (TestDialect.SupportsHavingWithoutGroupBy)
				{
					// Act
					s.CreateQuery("select max(a.BodyWeight) from Animal a having max(a.BodyWeight)>0").UniqueResultAsync()
						.ContinueWith(task =>
						{
							result = task.Result;

							// Assert
							Assert.That(result.GetType(), Is.InstanceOf(typeof(float)));
							Assert.That(result, Is.EqualTo(20F));
						}).Wait();
				}
			}
		}

		[Test]
		public void AggregateMinAsync()
		{
			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("a1", 20);
				Animal a2 = new Animal("a2", 10);
				s.Save(a1);
				s.Save(a2);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				object result = null;
				// Act
				s.CreateQuery("select min(a.BodyWeight) from Animal a").UniqueResultAsync()
					.ContinueWith(task =>
					{
						result = task.Result;

						// Assert
						Assert.That(result.GetType(), Is.InstanceOf(typeof(float)));
						Assert.That(result, Is.EqualTo(10F));
					}).Wait();
				

				if (TestDialect.SupportsHavingWithoutGroupBy)
				{
					// Act
					s.CreateQuery("select min(a.BodyWeight) from Animal a having min(a.BodyWeight)>0").UniqueResultAsync()
						.ContinueWith(task =>
						{
							result = task.Result;

							// Assert
							Assert.That(result.GetType(), Is.InstanceOf(typeof(float)));
							Assert.That(result, Is.EqualTo(10F));
						}).Wait();
				}
			}
		}

		[Test]
		public void AggregateSumAsync()
		{
			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("a1", 20);
				Animal a2 = new Animal("a2", 10);
				s.Save(a1);
				s.Save(a2);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				object result = null;
				// Act
				s.CreateQuery("select sum(a.BodyWeight) from Animal a").UniqueResultAsync()
					.ContinueWith(task =>
					{
						result = task.Result;

						// Assert
						Assert.AreEqual(typeof(double), result.GetType());
						Assert.AreEqual(30D, result);
					}).Wait();

				if (TestDialect.SupportsHavingWithoutGroupBy)
				{
					// Act
					s.CreateQuery("select sum(a.BodyWeight) from Animal a having sum(a.BodyWeight)>0").UniqueResultAsync()
						.ContinueWith(task =>
						{
							result = task.Result;

							// Assert
							Assert.AreEqual(typeof(double), result.GetType());
							Assert.AreEqual(30D, result);
						}).Wait();
				}
			}
		}

		[Test]
		public void AggregateSumArrangeAsync()
		{
			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("a1", 20);
				Animal a2 = new Animal("a2", 10);
				s.Save(a1);
				s.Save(a2);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				object result = null;
				// Act
				s.CreateQuery("select sum(a.BodyWeight) from Animal a").UniqueResultAsync()
					.ContinueWith(task =>
					{
						result = task.Result;

						// Assert
						Assert.That(result.GetType(), Is.InstanceOf(typeof(double)));
						Assert.That(result, Is.EqualTo(30D));
					}).Wait();

				if (TestDialect.SupportsHavingWithoutGroupBy)
				{
					// Act
					s.CreateQuery("select sum(a.BodyWeight) from Animal a having sum(a.BodyWeight)>0").UniqueResultAsync()
						.ContinueWith(task =>
						{
							result = task.Result;

							// Assert
							Assert.That(result.GetType(), Is.InstanceOf(typeof(double)));
							Assert.That(result, Is.EqualTo(30D));
						}).Wait();
				}
			}
		}

		[Test]
		public void SubStringTwoParametersAsync()
		{
			// All dialects that support the substring function should support
			// the two-parameter overload - emulating it by generating the 
			// third parameter (length) if the database requires three parameters.

			IgnoreIfNotSupported("substring");

			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("abcdef", 20);
				s.Save(a1);
				s.Flush();
			}

			using (ISession s = OpenSession())
			{
				string hql;

				// In the select clause.
				hql = "select substring(a.Description, 3) from Animal a";
				// Act
				IList lresult = s.CreateQuery(hql).List();
				// Assert
				Assert.AreEqual(1, lresult.Count);
				Assert.AreEqual("cdef", lresult[0]);

				// In the where clause.
				hql = "from Animal a where substring(a.Description, 4) = 'def'";
				Animal result = null;
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						result = (Animal) task.Result;
					}).Wait();
				// Assert
				Assert.That(result.Description, Is.EqualTo("abcdef"));

				// With parameters and nested function calls.
				if (!(Dialect is FirebirdDialect))  // Firebird only supports integer literals for start (and length).
				{
					hql = "from Animal a where substring(concat(a.Description, ?), :start) = 'deffoo'";
					// Act
					s.CreateQuery(hql)
						.SetParameter(0, "foo")
						.SetParameter("start", 4)
						.UniqueResultAsync()
						.ContinueWith(task =>
						{
							result = (Animal) task.Result;
							// Assert
							Assert.That(result.Description, Is.EqualTo("abcdef"));
						}).Wait();
				}
			}
		}


		[Test]
		public void SubStringAsync()
		{
			// Arrange
			IgnoreIfNotSupported("substring");
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("abcdef", 20);
				s.Save(a1);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				string hql;

				hql = "from Animal a where substring(a.Description, 2, 3) = 'bcd'";
				Animal result = null;
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						result = (Animal) task.Result;
						// Assert
						Assert.That(result.Description, Is.EqualTo("abcdef"));
					}).Wait();

				hql = "from Animal a where substring(a.Description, 2, 3) = ?";
				// Act
				s.CreateQuery(hql)
					.SetParameter(0, "bcd")
					.UniqueResultAsync()
					.ContinueWith(task =>
					{
						result = (Animal) task.Result;
						// Assert
						Assert.That(result.Description, Is.EqualTo("abcdef"));
					}).Wait();


				if (Dialect is FirebirdDialect)
				{
					// Firebird only supports integer literals for start (and length).
					return;
				}

				// Following tests verify that parameters can be used.

				hql = "from Animal a where substring(a.Description, 2, ?) = 'bcd'";
				// Act
				s.CreateQuery(hql)
					.SetParameter(0, 3)
					.UniqueResultAsync()
					.ContinueWith(task =>
					{
						result = (Animal) task.Result;
						// Assert
						Assert.That(result.Description, Is.EqualTo("abcdef"));
					}).Wait();

				hql = "from Animal a where substring(a.Description, ?, ?) = ?";
				// Act
				s.CreateQuery(hql)
					.SetParameter(0, 2)
					.SetParameter(1, 3)
					.SetParameter(2, "bcd")
					.UniqueResultAsync()
					.ContinueWith(task =>
					{
						result = (Animal) task.Result;
						// Assert
						Assert.That(result.Description, Is.EqualTo("abcdef"));
					}).Wait();

				hql = "select substring(a.Description, ?, ?) from Animal a";
				// Act
				IList results = s.CreateQuery(hql)
					.SetParameter(0, 2)
					.SetParameter(1, 3)
					.List();
				// Assert
				Assert.That(results.Count, Is.EqualTo(1));
				Assert.That(results[0], Is.EqualTo("bcd"));
			}
		}

		[Test]
		public void LocateAsync()
		{
			// Arrange
			IgnoreIfNotSupported("locate");
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("abcdef", 20);
				s.Save(a1);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				string hql = "select locate('bc', a.Description, 2) from Animal a";
				// Act
				IList lresult = s.CreateQuery(hql).List();
				// Assert
				Assert.That(lresult[0], Is.EqualTo(2));

				hql = "from Animal a where locate('bc', a.Description) = 2";
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						Animal result = (Animal) task.Result;
						Assert.That(result.Description, Is.EqualTo("abcdef"));
					}).Wait();
			}
		}

		[Test]
		public void LengthAsync()
		{
			IgnoreIfNotSupported("length");

			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("12345", 20);
				Animal a2 = new Animal("1234", 20);
				s.Save(a1);
				s.Save(a2);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				string hql = "select length(a.Description) from Animal a where a.Description = '1234'";
				// Act
				IList lresult = s.CreateQuery(hql).List();
				// Assert
				Assert.AreEqual(4, lresult[0]);

				hql = "from Animal a where length(a.Description) = 5";
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						Animal result = (Animal) task.Result;

						// Assert
						Assert.AreEqual("12345", result.Description);
					}).Wait();
			}
		}

		[Test]
		public void ModAsync()
		{
			IgnoreIfNotSupported("mod");
			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("abcdef", 20);
				s.Save(a1);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				string hql = "select mod(cast(a.BodyWeight as int), 3) from Animal a";
				// Act
				IList lresult = s.CreateQuery(hql).List();
				// Assert
				Assert.That(lresult[0], Is.EqualTo(2));

				hql = "from Animal a where mod(20, 3) = 2";
				// Act
				Animal result = null;
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						result = (Animal) task.Result;

						// Assert
						Assert.That(result.Description, Is.EqualTo("abcdef"));
					}).Wait();

				hql = "from Animal a where mod(cast(a.BodyWeight as int), 4)=0";
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						result = (Animal) task.Result;

						// Assert
						Assert.AreEqual("abcdef", result.Description);
					}).Wait();
			}
		}

		[Test]
		public void SqrtAsync()
		{
			IgnoreIfNotSupported("sqrt");
			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("abcdef", 65536f);
				s.Save(a1);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				string hql = "select sqrt(an.BodyWeight) from Animal an";
				// Act
				IList lresult = s.CreateQuery(hql).List();
				// Assert
				Assert.That(lresult[0], Is.EqualTo(256f));

				hql = "from Animal an where sqrt(an.BodyWeight)/2 > 10";
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						Animal result = (Animal) task.Result;

						// Assert
						Assert.That(result.Description, Is.EqualTo("abcdef"));
					}).Wait();
			}
		}

		[Test]
		public void UpperAsync()
		{
			IgnoreIfNotSupported("upper");
			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("abcdef", 1f);
				s.Save(a1);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				string hql = "select upper(an.Description) from Animal an";
				// Act
				IList lresult = s.CreateQuery(hql).List();
				// Assert
				Assert.That(lresult[0], Is.EqualTo("ABCDEF"));

				hql = "from Animal an where upper(an.Description)='ABCDEF'";
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						Animal result = (Animal) task.Result;

						// Assert
						Assert.That(result.Description, Is.EqualTo("abcdef"));
					}).Wait();
				
				//test only parser
				hql = "select upper(an.Description) from Animal an group by upper(an.Description) having upper(an.Description)='ABCDEF'";
				// Act
				lresult = s.CreateQuery(hql).List();
			}
		}

		[Test]
		public void LowerAsync()
		{
			IgnoreIfNotSupported("lower");
			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("ABCDEF", 1f);
				s.Save(a1);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				string hql = "select lower(an.Description) from Animal an";
				// Act
				IList lresult = s.CreateQuery(hql).List();
				// Assert
				Assert.That(lresult[0], Is.EqualTo("abcdef"));

				hql = "from Animal an where lower(an.Description)='abcdef'";
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						Animal result = (Animal) task.Result;

						// Assert
						Assert.That(result.Description, Is.EqualTo("ABCDEF"));
					}).Wait();

				//test only parser
				hql = "select lower(an.Description) from Animal an group by lower(an.Description) having lower(an.Description)='abcdef'";
				// Act
				lresult = s.CreateQuery(hql).List();
			}
		}

		[Test]
		public void StrAsync()
		{
			IgnoreIfNotSupported("str");
			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("abcdef", 20);
				s.Save(a1);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				string hql = "select str(a.BodyWeight) from Animal a";
				// Act
				IList lresult = s.CreateQuery(hql).List();
				// Assert
				Assert.AreEqual(typeof(string), lresult[0].GetType());

				hql = "from Animal a where str(123) = '123'";
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						Animal result = (Animal) task.Result;
						// Assert
						Assert.That(result.Description, Is.EqualTo("abcdef"));
					}).Wait();
			}
		}

		[Test]
		public void IifAsync()
		{
			if (!Dialect.Functions.ContainsKey("iif"))
				Assert.Ignore(Dialect + "doesn't support iif function.");

			// Arrange
			using (ISession s = OpenSession())
			{
				s.Save(new MaterialResource("Flash card 512MB", "A001/07", MaterialResource.MaterialState.Available));
				s.Save(new MaterialResource("Flash card 512MB", "A002/07", MaterialResource.MaterialState.Available));
				s.Save(new MaterialResource("Flash card 512MB", "A003/07", MaterialResource.MaterialState.Reserved));
				s.Save(new MaterialResource("Flash card 512MB", "A004/07", MaterialResource.MaterialState.Reserved));
				s.Save(new MaterialResource("Flash card 512MB", "A005/07", MaterialResource.MaterialState.Discarded));
				s.Flush();
			}

			// Statistic
			using (ISession s = OpenSession())
			{
				string hql = 
@"select mr.Description, 
sum(iif(mr.State= 0,1,0)), 
sum(iif(mr.State= 1,1,0)), 
sum(iif(mr.State= 2,1,0)) 
from MaterialResource mr
group by mr.Description";

				// Act
				IList lresult = s.CreateQuery(hql).List();

				// Assert
				Assert.That(((IList)lresult[0])[0], Is.EqualTo("Flash card 512MB"));
				Assert.That(((IList)lresult[0])[1], Is.EqualTo(2));
				Assert.That(((IList)lresult[0])[2], Is.EqualTo(2));
				Assert.That(((IList)lresult[0])[3], Is.EqualTo(1));

				hql = "from MaterialResource mr where iif(mr.State=2,true,false)=true";
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						MaterialResource result = (MaterialResource) task.Result;
						
						// Assert
						Assert.That(result.SerialNumber, Is.EqualTo("A005/07"));
					}).Wait();
			}
			// clean up
			using (ISession s = OpenSession())
			{
				s.Delete("from MaterialResource");
				s.Flush();
			}
		}
	}
}
