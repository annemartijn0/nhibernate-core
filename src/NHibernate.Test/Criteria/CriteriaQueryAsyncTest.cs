using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using NHibernate.Criterion;
using NHibernate.SqlCommand;
using NUnit.Framework;

namespace NHibernate.Test.Criteria
{
	[TestFixture]
	public class CriteriaQueryAsyncTest : TestCase
	{
		protected override string MappingsAssembly
		{
			get { return "NHibernate.Test"; }
		}

		protected override IList Mappings
		{
			get
			{
				return new string[]
					{
						"Criteria.Enrolment.hbm.xml",
						"Criteria.Animal.hbm.xml",
						"Criteria.MaterialResource.hbm.xml"
					};
			}
		}

		[Test]
		public void ListAsync_fetchesAllStudents()
		{
			// Assert
			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.Save(new Student { Name = "Mengano", StudentNumber = 232 });
				session.Save(new Student { Name = "Ayende", StudentNumber = 999 });
				session.Save(new Student { Name = "Fabio", StudentNumber = 123 });
				session.Save(new Student { Name = "Merlo", StudentNumber = 456 });
				session.Save(new Student { Name = "Fulano", StudentNumber = 0 });

				t.Commit();
			}

			using (ISession session = OpenSession())
			{
				// Act
				var result = session.CreateCriteria<Student>()
					.ListAsync<Student>().Result;

				// Assert
				Assert.AreEqual(result.Count, 5);
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.CreateQuery("delete from Student").ExecuteUpdate();
				t.Commit();
			}
		}

		[Test]
		public void ListAsync_fetchesAllStudentsMultipleTimes()
		{
			// Arrange
			const int numberOfTasks = 4;
			var tasks = new Task<IList<Student>>[numberOfTasks];

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.Save(new Student { Name = "Mengano", StudentNumber = 232 });
				session.Save(new Student { Name = "Ayende", StudentNumber = 999 });
				session.Save(new Student { Name = "Fabio", StudentNumber = 123 });
				session.Save(new Student { Name = "Merlo", StudentNumber = 456 });
				session.Save(new Student { Name = "Fulano", StudentNumber = 0 });

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				// Act
				for (int i = 0; i < numberOfTasks; i++)
				{
					var task = session.CreateCriteria<Student>()
					.ListAsync<Student>();
					tasks[i] = task;
				}

				// Assert: No Exceptions
				// Assert: all tasks return all students
				foreach (var task in tasks)
				{
					Assert.AreEqual(task.Result.Count, 5);
				}

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.CreateQuery("delete from Student").ExecuteUpdate();
				t.Commit();
			}
		}

		[Test]
		public void SubqueryPaginationOnlyWithFirstAsync()
		{
			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.Save(new Student { Name = "Mengano", StudentNumber = 232 });
				session.Save(new Student { Name = "Ayende", StudentNumber = 999 });
				session.Save(new Student { Name = "Fabio", StudentNumber = 123 });
				session.Save(new Student { Name = "Merlo", StudentNumber = 456 });
				session.Save(new Student { Name = "Fulano", StudentNumber = 0 });

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				DetachedCriteria dc = DetachedCriteria.For(typeof(Student))
					.Add(Property.ForName("StudentNumber").Gt(0L))
					.SetFirstResult(1)
					.AddOrder(Order.Asc("StudentNumber"))
					.SetProjection(Property.ForName("Name"));

				var result = session.CreateCriteria(typeof(Student))
					.Add(Subqueries.PropertyIn("Name", dc))
					.ListAsync<Student>().Result;

				Assert.That(result.Count, Is.EqualTo(3));
				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.CreateQuery("delete from Student").ExecuteUpdate();
				t.Commit();
			}
		}

		[Test]
		public void SubqueryPaginationAsync()
		{
			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.Save(new Student { Name = "Mengano", StudentNumber = 232 });
				session.Save(new Student { Name = "Ayende", StudentNumber = 999 });
				session.Save(new Student { Name = "Fabio", StudentNumber = 123 });
				session.Save(new Student { Name = "Merlo", StudentNumber = 456 });
				session.Save(new Student { Name = "Fulano", StudentNumber = 0 });

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				DetachedCriteria dc = DetachedCriteria.For(typeof(Student))
					.Add(Property.ForName("StudentNumber").Gt(200L))
					.SetMaxResults(2)
					.SetFirstResult(1)
					.AddOrder(Order.Asc("StudentNumber"))
					.SetProjection(Property.ForName("Name"));

				var result = session.CreateCriteria(typeof(Student))
					.Add(Subqueries.PropertyIn("Name", dc))
					.AddOrder(Order.Asc("StudentNumber"))
					.ListAsync<Student>().Result;

				Assert.That(result.Count, Is.EqualTo(2));
				Assert.That(result[0].StudentNumber, Is.EqualTo(456));
				Assert.That(result[1].StudentNumber, Is.EqualTo(999));

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.CreateQuery("delete from Student").ExecuteUpdate();
				t.Commit();
			}
		}

		[Test]
		public void SimplePaginationAsync()
		{
			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.Save(new Student { Name = "Mengano", StudentNumber = 232 });
				session.Save(new Student { Name = "Ayende", StudentNumber = 999 });
				session.Save(new Student { Name = "Fabio", StudentNumber = 123 });
				session.Save(new Student { Name = "Merlo", StudentNumber = 456 });
				session.Save(new Student { Name = "Fulano", StudentNumber = 0 });

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				var result = session.CreateCriteria<Student>()
					.Add(Restrictions.Gt("StudentNumber", 0L))
					.AddOrder(Order.Asc("StudentNumber"))
					.SetFirstResult(1).SetMaxResults(2)
					.ListAsync<Student>().Result;
				Assert.That(result.Count, Is.EqualTo(2));
				Assert.That(result[0].StudentNumber, Is.EqualTo(232));
				Assert.That(result[1].StudentNumber, Is.EqualTo(456));

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.CreateQuery("delete from Student").ExecuteUpdate();
				t.Commit();
			}
		}

		[Test]
		public void SimplePaginationOnlyWithFirstAsync()
		{
			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.Save(new Student { Name = "Mengano", StudentNumber = 232 });
				session.Save(new Student { Name = "Ayende", StudentNumber = 999 });
				session.Save(new Student { Name = "Fabio", StudentNumber = 123 });
				session.Save(new Student { Name = "Merlo", StudentNumber = 456 });
				session.Save(new Student { Name = "Fulano", StudentNumber = 0 });

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				var result = session.CreateCriteria<Student>()
					.Add(Restrictions.Gt("StudentNumber", 0L))
					.AddOrder(Order.Asc("StudentNumber"))
					.SetFirstResult(1)
					.ListAsync<Student>().Result;

				Assert.That(result.Count, Is.EqualTo(3));
				Assert.That(result[0].StudentNumber, Is.EqualTo(232));
				Assert.That(result[1].StudentNumber, Is.EqualTo(456));
				Assert.That(result[2].StudentNumber, Is.EqualTo(999));

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.CreateQuery("delete from Student").ExecuteUpdate();
				t.Commit();
			}
		}

		[Test]
		public void PropertyWithFormulaAndPagingAsyncTest()
		{
			ISession s = OpenSession();
			ITransaction t = s.BeginTransaction();

			ICriteria crit = s.CreateCriteria(typeof(Animal))
				.SetFirstResult(0)
				.SetMaxResults(1)
				.AddOrder(new Order("bodyWeight", true));

			crit.ListAsync<Animal>();

			t.Rollback();
			s.Close();
		}
	}
}