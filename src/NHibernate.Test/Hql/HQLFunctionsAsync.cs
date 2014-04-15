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
				if (dialects.Contains(Dialect.GetType()))
					Assert.Ignore(Dialect + " doesn't support " + functionName + " function.");
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
							Assert.That(result.GetType(), Is.InstanceOf(typeof(long)));
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
		public void AggregatesAndMathNH959Async()
		{
			// Arrange
			using (ISession s = OpenSession())
			{
				// Act && Assert
				Assert.DoesNotThrow(() => s.CreateQuery("select a.Id, sum(BodyWeight)/avg(BodyWeight) from Animal a group by a.Id having sum(BodyWeight)>0").ListAsync().Wait());
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
				s.CreateQuery(hql).ListAsync()
					.ContinueWith(task =>
					{
						IList lresult = task.Result;

						// Assert
						Assert.AreEqual(1, lresult.Count);
						Assert.AreEqual("cdef", lresult[0]);
					}).Wait();

				// In the where clause.
				hql = "from Animal a where substring(a.Description, 4) = 'def'";
				Animal result = null;
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						result = (Animal)task.Result;
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
							result = (Animal)task.Result;
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
						result = (Animal)task.Result;
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
						result = (Animal)task.Result;
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
						result = (Animal)task.Result;
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
						result = (Animal)task.Result;
						// Assert
						Assert.That(result.Description, Is.EqualTo("abcdef"));
					}).Wait();

				hql = "select substring(a.Description, ?, ?) from Animal a";
				// Act
				s.CreateQuery(hql)
					.SetParameter(0, 2)
					.SetParameter(1, 3)
					.ListAsync()
					.ContinueWith(task =>
					{
						IList results = task.Result;

						// Assert
						Assert.That(results.Count, Is.EqualTo(1));
						Assert.That(results[0], Is.EqualTo("bcd"));
					}).Wait();
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
				s.CreateQuery(hql).ListAsync()
					.ContinueWith(task =>
					{
						IList lresult = task.Result;

						// Assert
						Assert.That(lresult[0], Is.EqualTo(2));
					}).Wait();

				hql = "from Animal a where locate('bc', a.Description) = 2";
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						Animal result = (Animal)task.Result;
						Assert.That(result.Description, Is.EqualTo("abcdef"));
					}).Wait();
			}
		}

		[Test]
		public void TrimAsync()
		{
			IgnoreIfNotSupported("trim");

			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("abc   ", 1);
				Animal a2 = new Animal("   def", 2);
				Animal a3 = new Animal("___def__", 3);
				s.Save(a1);
				s.Save(a2);
				s.Save(a3);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				string hql = "select trim(a.Description) from Animal a where a.Description='   def'";
				// Act
				s.CreateQuery(hql).ListAsync()
					.ContinueWith(task =>
						// Assert
						Assert.AreEqual("def", task.Result[0])).Wait();

				hql = "select trim('_' from a.Description) from Animal a where a.Description='___def__'";
				// Act
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					// Assert
					Assert.AreEqual("def", task.Result[0])).Wait();

				hql = "select trim(trailing from a.Description) from Animal a where a.Description= 'abc   '";
				// Act
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					// Assert
					Assert.AreEqual("abc", task.Result[0])).Wait();

				hql = "select trim(leading from a.Description) from Animal a where a.Description='   def'";
				// Act
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					// Assert
					Assert.AreEqual("def", task.Result[0])).Wait();

				// where
				hql = "from Animal a where trim(a.Description) = 'abc'";
				// Act
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					// Assert
					Assert.AreEqual(1, task.Result.Count)).Wait();

				hql = "from Animal a where trim('_' from a.Description) = 'def'";
				// Act
				s.CreateQuery(hql).UniqueResultAsync().ContinueWith(task =>
				{
					Animal result = (Animal)task.Result;

					// Assert
					Assert.AreEqual("___def__", result.Description);
				}).Wait();

				hql = "from Animal a where trim(trailing from a.Description) = 'abc'";
				// Act
				s.CreateQuery(hql).UniqueResultAsync().ContinueWith(task =>
				{
					var result = (Animal)task.Result;

					// Assert
					Assert.AreEqual(1, result.BodyWeight); //Firebird auto rtrim VARCHAR
				}).Wait();


				hql = "from Animal a where trim(leading from a.Description) = 'def'";
				// Act
				s.CreateQuery(hql).UniqueResultAsync().ContinueWith(task =>
				{
					var result = (Animal)task.Result;

					// Assert
					Assert.AreEqual("   def", result.Description);
				}).Wait();

				Animal a = new Animal("   abc", 20);
				s.Save(a);
				s.Flush();
				hql = "from Animal a where trim(both from a.Description) = 'abc'";
				// Act
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					// Assert
					Assert.AreEqual(2, task.Result.Count)).Wait();
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
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					// Assert
					Assert.AreEqual(4, task.Result[0])).Wait();

				hql = "from Animal a where length(a.Description) = 5";
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						Animal result = (Animal)task.Result;

						// Assert
						Assert.AreEqual("12345", result.Description);
					}).Wait();
			}
		}

		[Test]
		public void Bit_lengthAsync()
		{
			IgnoreIfNotSupported("bit_length");

			// test only the parser
			using (ISession s = OpenSession())
			{
				string hql = "from Animal a where bit_length(a.Description) = 24";
				s.CreateQuery(hql).ListAsync().Wait();

				hql = "select bit_length(a.Description) from Animal a";
				s.CreateQuery(hql).ListAsync().Wait();
			}
		}

		[Test]
		public void CoalesceAsync()
		{
			IgnoreIfNotSupported("coalesce");
			// test only the parser and render
			using (ISession s = OpenSession())
			{
				string hql = "select coalesce(h.NickName, h.Name.First, h.Name.Last) from Human h";
				s.CreateQuery(hql).ListAsync().Wait();

				hql = "from Human h where coalesce(h.NickName, h.Name.First, h.Name.Last) = 'max'";
				s.CreateQuery(hql).ListAsync().Wait();
			}
		}

		[Test]
		public void NullifAsync()
		{
			IgnoreIfNotSupported("nullif");
			// Arrange
			string hql1, hql2;
			if (!IsOracleDialect())
			{
				hql1 = "select nullif(h.NickName, '1e1') from Human h";
				hql2 = "from Human h where not(nullif(h.NickName, '1e1') is null)";
			}
			else
			{
				// Oracle need same specific types
				hql1 = "select nullif(str(h.NickName), '1e1') from Human h";
				hql2 = "from Human h where not (nullif(str(h.NickName), '1e1') is null)";
			}
			// test only the parser and render
			using (ISession s = OpenSession())
			{
				// Act
				s.CreateQuery(hql1).ListAsync().Wait();
				s.CreateQuery(hql2).ListAsync().Wait();
			}
		}

		[Test]
		public void AbsAsync()
		{
			IgnoreIfNotSupported("abs");
			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("Dog", 9);
				s.Save(a1);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				string hql = "select abs(a.BodyWeight*-1) from Animal a";
				// Act
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					// Assert
					Assert.AreEqual(9, task.Result[0])).Wait();

				hql = "from Animal a where abs(a.BodyWeight*-1)>0";
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					// Assert
					Assert.AreEqual(1, task.Result.Count)).Wait();

				hql = "select abs(a.BodyWeight*-1) from Animal a group by abs(a.BodyWeight*-1) having abs(a.BodyWeight*-1)>0";
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					// Assert
					Assert.AreEqual(1, task.Result.Count)).Wait();
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
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					// Assert
					Assert.That(task.Result[0], Is.EqualTo(2))).Wait();

				hql = "from Animal a where mod(20, 3) = 2";
				// Act
				Animal result = null;
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						result = (Animal)task.Result;

						// Assert
						Assert.That(result.Description, Is.EqualTo("abcdef"));
					}).Wait();

				hql = "from Animal a where mod(cast(a.BodyWeight as int), 4)=0";
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						result = (Animal)task.Result;

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
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					// Assert
					Assert.That(task.Result[0], Is.EqualTo(256f))).Wait();

				hql = "from Animal an where sqrt(an.BodyWeight)/2 > 10";
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						Animal result = (Animal)task.Result;

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
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					// Assert
					Assert.That(task.Result[0], Is.EqualTo("ABCDEF"))).Wait();

				hql = "from Animal an where upper(an.Description)='ABCDEF'";
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						Animal result = (Animal)task.Result;

						// Assert
						Assert.That(result.Description, Is.EqualTo("abcdef"));
					}).Wait();

				//test only parser
				hql = "select upper(an.Description) from Animal an group by upper(an.Description) having upper(an.Description)='ABCDEF'";
				// Act
				s.CreateQuery(hql).ListAsync().Wait();
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
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					// Assert
					Assert.That(task.Result[0], Is.EqualTo("abcdef"))).Wait();

				hql = "from Animal an where lower(an.Description)='abcdef'";
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						Animal result = (Animal)task.Result;

						// Assert
						Assert.That(result.Description, Is.EqualTo("ABCDEF"));
					}).Wait();

				//test only parser
				hql = "select lower(an.Description) from Animal an group by lower(an.Description) having lower(an.Description)='abcdef'";
				// Act
				s.CreateQuery(hql).ListAsync().Wait();
			}
		}

		[Test]
		public void CastAsync()
		{
			// Arrange
			const double magicResult = 7 + 123 - 5 * 1.3d;

			IgnoreIfNotSupported("cast");
			// The cast is used to test various cases of a function render
			// Cast was selected because represent a special case for:
			// 1) Has more then 1 argument
			// 2) The argument separator is "as" (for the other function is ',' or ' ')
			// 3) The ReturnType is not fixed (depend on a column type)
			// 4) The 2th argument is parsed by NH and traslated for a specific Dialect (can't be interpreted directly by RDBMS)
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("abcdef", 1.3f);
				s.Save(a1);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				string hql;
				IList l;
				Animal result;
				// Rendered in SELECT using a property 
				hql = "select cast(a.BodyWeight as Double) from Animal a";
				// Act
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
				{
					l = task.Result;
					// Assert
					Assert.AreEqual(1, l.Count);
					Assert.That(l[0], Is.TypeOf(typeof(double)));
				}).Wait();

				// Rendered in SELECT using a property in an operation with costant 
				hql = "select cast(7+123-5*a.BodyWeight as Double) from Animal a";
				// Act
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
				{
					l = task.Result;
					// Assert
					Assert.AreEqual(1, l.Count);
					Assert.AreEqual(magicResult, (double)l[0], 0.00001);
				}).Wait();


				// Rendered in SELECT using a property and nested functions
				if (!(Dialect is Oracle8iDialect))
				{
					hql = "select cast(cast(a.BodyWeight as string) as Double) from Animal a";
					s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					{
						l = task.Result;
						// Assert
						Assert.AreEqual(1, l.Count);
						Assert.That(l[0], Is.TypeOf(typeof(double)));
					}).Wait();
				}

				// TODO: Rendered in SELECT using string costant assigned with critic chars (separators)

				// Rendered in WHERE using a property 
				if (!(Dialect is Oracle8iDialect))
				{
					hql = "from Animal a where cast(a.BodyWeight as string) like '1.%'";
					// Act
					s.CreateQuery(hql).UniqueResultAsync()
						.ContinueWith(task =>
						{
							result = (Animal)task.Result;
							// Assert
							Assert.AreEqual("abcdef", result.Description);
						}).Wait();
				}

				// Rendered in WHERE using a property in an operation with costants
				hql = "from Animal a where cast(7+123-2*a.BodyWeight as Double)>0";
				// Act
				s.CreateQuery(hql).UniqueResultAsync().ContinueWith(task =>
				{
					result = (Animal)task.Result;
					// Assert
					Assert.AreEqual("abcdef", result.Description);
				}).Wait();

				// Rendered in WHERE using a property and named param
				hql = "from Animal a where cast(:aParam+a.BodyWeight as Double)>0";
				// Act
				s.CreateQuery(hql)
					.SetDouble("aParam", 2D)
					.UniqueResultAsync()
					.ContinueWith(task =>
					{
						result = (Animal)task.Result;
						// Assert
						Assert.AreEqual("abcdef", result.Description);
					}).Wait();

				// Rendered in WHERE using a property and nested functions
				if (!(Dialect is Oracle8iDialect))
				{
					hql = "from Animal a where cast(cast(cast(a.BodyWeight as string) as double) as int) = 1";
					// Act
					s.CreateQuery(hql).UniqueResultAsync().ContinueWith(task =>
					{
						result = (Animal)task.Result;

						// Assert
						Assert.AreEqual("abcdef", result.Description);
					}).Wait();
				}

				// Rendered in GROUP BY using a property 
				hql = "select cast(a.BodyWeight as Double) from Animal a group by cast(a.BodyWeight as Double)";
				// Act
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
				{
					l = task.Result;
					// Assert
					Assert.AreEqual(1, l.Count);
					Assert.That(l[0], Is.TypeOf(typeof(double)));
				}).Wait();

				// Rendered in GROUP BY using a property in an operation with costant 
				hql = "select cast(7+123-5*a.BodyWeight as Double) from Animal a group by cast(7+123-5*a.BodyWeight as Double)";
				// Act
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					{
						l = task.Result;
						// Assert
						Assert.AreEqual(1, l.Count);
						Assert.AreEqual(magicResult, (double)l[0], 0.00001);
					}).Wait();

				// Rendered in GROUP BY using a property and nested functions
				if (!(Dialect is Oracle8iDialect))
				{
					hql = "select cast(cast(a.BodyWeight as string) as Double) from Animal a group by cast(cast(a.BodyWeight as string) as Double)";
					// Act
					s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					{
						l = task.Result;
						// Assert
						Assert.AreEqual(1, l.Count);
						Assert.That(l[0], Is.TypeOf(typeof(double)));
					}).Wait();

				}

				// Rendered in HAVING using a property 
				hql = "select cast(a.BodyWeight as Double) from Animal a group by cast(a.BodyWeight as Double) having cast(a.BodyWeight as Double)>0";
				// Act
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
				{
					l = task.Result;
					// Assert
					Assert.AreEqual(1, l.Count);
					Assert.That(l[0], Is.TypeOf(typeof(double)));
				}).Wait();


				// Rendered in HAVING using a property in an operation with costants
				hql = "select cast(7+123.3-1*a.BodyWeight as int) from Animal a group by cast(7+123.3-1*a.BodyWeight as int) having cast(7+123.3-1*a.BodyWeight as int)>0";
				// Act
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
				{
					l = task.Result;
					// Assert
					Assert.AreEqual(1, l.Count);
					Assert.AreEqual((int)(7 + 123.3 - 1 * 1.3d), l[0]);
				}).Wait();

				// Rendered in HAVING using a property and named param (NOT SUPPORTED)
				try
				{
					hql = "select cast(:aParam+a.BodyWeight as int) from Animal a group by cast(:aParam+a.BodyWeight as int) having cast(:aParam+a.BodyWeight as int)>0";
					// Act
					s.CreateQuery(hql).SetInt32("aParam", 10).ListAsync().ContinueWith(task =>
					{
						l = task.Result;
						// Assert
						Assert.AreEqual(1, l.Count);
						Assert.AreEqual(11, l[0]);
					}).Wait();

				}
				catch (QueryException ex)
				{
					if (!(ex.InnerException is NotSupportedException))
						throw;
				}
				catch (ADOException ex)
				{
					if (Dialect is Oracle8iDialect)
					{
						if (!ex.InnerException.Message.StartsWith("ORA-00979"))
							throw;
					}
					else
					{
						string msgToCheck =
							"Column 'Animal.BodyWeight' is invalid in the HAVING clause because it is not contained in either an aggregate function or the GROUP BY clause.";
						// This test raises an exception in SQL Server because named 
						// parameters internally are always positional (@p0, @p1, etc.)
						// and named differently hence they mismatch between GROUP BY and HAVING clauses.
						if (!ex.InnerException.Message.Equals(msgToCheck))
							throw;
					}
				}

				// Rendered in HAVING using a property and nested functions
				if (!(Dialect is Oracle8iDialect))
				{
					string castExpr = "cast(cast(cast(a.BodyWeight as string) as double) as int)";
					hql = string.Format("select {0} from Animal a group by {0} having {0} = 1", castExpr);
					// Act
					s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					{
						l = task.Result;
						// Assert
						Assert.AreEqual(1, l.Count);
						Assert.AreEqual(1, l[0]);
					}).Wait();
				}
			}
		}

		[Test]
		public void CastNH1446Async()
		{
			IgnoreIfNotSupported("cast");
			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("abcdef", 1.3f);
				s.Save(a1);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				// Rendered in SELECT using a property 
				string hql = "select cast(a.BodyWeight As Double) from Animal a";
				// Act
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
				{
					IList l = task.Result;
					// Assert
					Assert.AreEqual(1, l.Count);
					Assert.AreEqual(1.3f, (double)l[0], 0.00001);
				}).Wait();
			}
		}

		[Test]
		public void CastNH1979Async()
		{
			IgnoreIfNotSupported("cast");
			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("abcdef", 1.3f);
				s.Save(a1);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				string hql = "select cast(((a.BodyWeight + 50) / :divisor) as int) from Animal a";
				// Act
				s.CreateQuery(hql).SetInt32("divisor", 2).ListAsync().ContinueWith(task =>
				{
					IList l = task.Result;
					// Assert
					Assert.AreEqual(1, l.Count);
				}).Wait();
			}
		}

		[Test]
		public void Current_TimeStampAsync()
		{
			IgnoreIfNotSupported("current_timestamp");
			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("abcdef", 1.3f);
				s.Save(a1);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				string hql = "select current_timestamp() from Animal";
				// Act && Assert
				s.CreateQuery(hql).ListAsync().Wait();
			}
		}

		/// <summary>
		/// NH-1658 Async
		/// </summary>
		[Test]
		public void Current_TimeStamp_OffsetAsync()
		{
			if (!Dialect.Functions.ContainsKey("current_timestamp_offset"))
				Assert.Ignore(Dialect + " doesn't support current_timestamp_offset function");
			IgnoreIfNotSupported("current_timestamp_offset");
			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("abcdef", 1.3f);
				s.Save(a1);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				string hql = "select current_timestamp_offset() from Animal";
				// Act && Assert
				s.CreateQuery(hql).ListAsync().Wait();
			}
		}

		[Test]
		public void ExtractAsync()
		{
			IgnoreIfNotSupported("extract");

			// test only the parser and render
			// Arrange
			using (ISession s = OpenSession())
			{
				string hql = "select extract(second from current_timestamp()), extract(minute from current_timestamp()), extract(hour from current_timestamp()) from Animal";
				// Act && Assert
				s.CreateQuery(hql).ListAsync().Wait();

				hql = "from Animal where extract(day from cast(current_timestamp() as Date))>0";
				// Act && Assert
				s.CreateQuery(hql).ListAsync().Wait();
			}
		}

		[Test]
		public void ConcatAsync()
		{
			IgnoreIfNotSupported("concat");
			// Arrange
			using (ISession s = OpenSession())
			{
				Animal a1 = new Animal("abcdef", 1f);
				s.Save(a1);
				s.Flush();
			}
			using (ISession s = OpenSession())
			{
				string hql = "select concat(a.Description,'zzz') from Animal a";
				// Act
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					// Assert
					Assert.AreEqual("abcdefzzz", task.Result[0])).Wait();

				// MS SQL doesn't support || operator
				if (!(Dialect is MsSql2000Dialect))
				{
					hql = "from Animal a where a.Description = concat('a', concat('b','c'), 'd'||'e')||'f'";
					s.CreateQuery(hql).UniqueResultAsync().ContinueWith(task =>
					{
						Animal result = (Animal) task.Result;
						// Assert
						Assert.AreEqual("abcdef", result.Description);
					}).Wait();
				}
			}
		}

		[Test]
		public void HourMinuteSecondAsync()
		{
			IgnoreIfNotSupported("second");
			// test only the parser and render
			// Arrange
			using (ISession s = OpenSession())
			{
				string hql = "select second(current_timestamp()), minute(current_timestamp()), hour(current_timestamp()) from Animal";
				// Act && Assert
				s.CreateQuery(hql).ListAsync().Wait();
			}
		}

		[Test]
		public void DayMonthYearAsync()
		{
			IgnoreIfNotSupported("day");
			IgnoreIfNotSupported("month");
			IgnoreIfNotSupported("year");
			// test only the parser and render
			// Arrange
			using (ISession s = OpenSession())
			{
				string hql = "select day(h.Birthdate), month(h.Birthdate), year(h.Birthdate) from Human h";
				// Act && Assert
				s.CreateQuery(hql).ListAsync().Wait();
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
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
					// Assert
					Assert.AreEqual(typeof(string), task.Result[0].GetType())).Wait();

				hql = "from Animal a where str(123) = '123'";
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						Animal result = (Animal)task.Result;
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
				s.CreateQuery(hql).ListAsync().ContinueWith(task =>
				{
					IList lresult = task.Result;

					// Assert
					Assert.That(((IList)lresult[0])[0], Is.EqualTo("Flash card 512MB"));
					Assert.That(((IList)lresult[0])[1], Is.EqualTo(2));
					Assert.That(((IList)lresult[0])[2], Is.EqualTo(2));
					Assert.That(((IList)lresult[0])[3], Is.EqualTo(1));
				}).Wait();

				hql = "from MaterialResource mr where iif(mr.State=2,true,false)=true";
				// Act
				s.CreateQuery(hql).UniqueResultAsync()
					.ContinueWith(task =>
					{
						MaterialResource result = (MaterialResource)task.Result;

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

		[Test]
		public void NH1725Async()
		{
			// Only to test the parser
			// Arrange
			using (ISession s = OpenSession())
			{
				var hql = "select new ForNh1725(mr.Description, iif(mr.State= 0,1,0)) from MaterialResource mr";
				// Act && Assert
				s.CreateQuery(hql).ListAsync().Wait();
				hql = "select new ForNh1725(mr.Description, cast(iif(mr.State= 0,1,0) as int)) from MaterialResource mr";
				// Act && Assert
				s.CreateQuery(hql).ListAsync().Wait();
			}
		}
	}
}
