using System;
using System.Collections;
using System.Collections.Generic;
using NHibernate.Dialect;
using NHibernate.Criterion;
using NHibernate.SqlCommand;
using NHibernate.Transform;
using NHibernate.Type;
using NHibernate.Util;
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
				DetachedCriteria dc = DetachedCriteria.For(typeof (Student))
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
				session.Save(new Student {Name = "Mengano", StudentNumber = 232});
				session.Save(new Student {Name = "Ayende", StudentNumber = 999});
				session.Save(new Student {Name = "Fabio", StudentNumber = 123});
				session.Save(new Student {Name = "Merlo", StudentNumber = 456});
				session.Save(new Student {Name = "Fulano", StudentNumber = 0});

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
				session.Save(new Student {Name = "Mengano", StudentNumber = 232});
				session.Save(new Student {Name = "Ayende", StudentNumber = 999});
				session.Save(new Student {Name = "Fabio", StudentNumber = 123});
				session.Save(new Student {Name = "Merlo", StudentNumber = 456});
				session.Save(new Student {Name = "Fulano", StudentNumber = 0});

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

		[Test]
		public void AliasJoinCriterionAsync()
		{
			using (ISession session = this.OpenSession())
			{
				using (ITransaction t = session.BeginTransaction())
				{
					Course courseA = new Course();
					courseA.CourseCode = "HIB-A";
					courseA.Description = "Hibernate Training A";
					session.Persist(courseA);
					
					Course courseB = new Course();
					courseB.CourseCode = "HIB-B";
					courseB.Description = "Hibernate Training B";
					session.Persist(courseB);

					Student gavin = new Student();
					gavin.Name = "Gavin King";
					gavin.StudentNumber = 232;
					gavin.PreferredCourse = courseA;
					session.Persist(gavin);

					Student leonardo = new Student();
					leonardo.Name = "Leonardo Quijano";
					leonardo.StudentNumber = 233;
					leonardo.PreferredCourse = courseB;
					session.Persist(leonardo);

					Student johnDoe = new Student();
					johnDoe.Name = "John Doe";
					johnDoe.StudentNumber = 235;
					johnDoe.PreferredCourse = null;
					session.Persist(johnDoe);

					// test == on one value exists
					IList<string> result = session.CreateCriteria<Student>()
						.CreateAlias("PreferredCourse", "pc", JoinType.LeftOuterJoin,
							Restrictions.Eq("pc.CourseCode", "HIB-A"))
						.SetProjection(Property.ForName("pc.CourseCode"))
						.AddOrder(Order.Asc("pc.CourseCode"))
						.ListAsync<string>().Result;
					
					// can't be sure of NULL comparison ordering aside from they should
					// either come first or last
					if (result[0] == null)
					{
						Assert.IsNull(result[1]);
						Assert.AreEqual("HIB-A", result[2]);
					}
					else
					{
						Assert.IsNull(result[2]);
						Assert.IsNull(result[1]);
						Assert.AreEqual("HIB-A", result[0]);
					}

					// test == on non existent value
					result = session.CreateCriteria<Student>()
						.CreateAlias("PreferredCourse", "pc", JoinType.LeftOuterJoin,
							Restrictions.Eq("pc.CourseCode", "HIB-R"))
						.SetProjection(Property.ForName("pc.CourseCode"))
						.AddOrder(Order.Asc("pc.CourseCode"))
						.List<string>();

					Assert.AreEqual(3, result.Count);
					Assert.IsNull(result[2]);
					Assert.IsNull(result[1]);
					Assert.IsNull(result[0]);

					// test != on one existing value
					result = session.CreateCriteria<Student>()
						.CreateAlias("PreferredCourse", "pc", JoinType.LeftOuterJoin,
							Restrictions.Not(Restrictions.Eq("pc.CourseCode", "HIB-A")))
						.SetProjection(Property.ForName("pc.CourseCode"))
						.AddOrder(Order.Asc("pc.CourseCode"))
						.List<string>();

					Assert.AreEqual(3, result.Count);

					// can't be sure of NULL comparison ordering aside from they should
					// either come first or last
					if (result[0] == null)
					{
						Assert.IsNull(result[1]);
						Assert.AreEqual("HIB-B", result[2]);
					}
					else
					{
						Assert.AreEqual("HIB-B", result[0]);
						Assert.IsNull(result[1]);
						Assert.IsNull(result[2]);
					}

					// test != on one existing value (using clone)
					var criteria = session.CreateCriteria<Student>()
						.CreateAlias("PreferredCourse", "pc", JoinType.LeftOuterJoin,
						             Restrictions.Not(Restrictions.Eq("pc.CourseCode", "HIB-A")))
						.SetProjection(Property.ForName("pc.CourseCode"))
						.AddOrder(Order.Asc("pc.CourseCode"));
					var clonedCriteria = CriteriaTransformer.Clone(criteria);
					result = clonedCriteria.List<string>();

					Assert.AreEqual(3, result.Count);

					// can't be sure of NULL comparison ordering aside from they should
					// either come first or last
					if (result[0] == null)
					{
						Assert.IsNull(result[1]);
						Assert.AreEqual("HIB-B", result[2]);
					}
					else
					{
						Assert.AreEqual("HIB-B", result[0]);
						Assert.IsNull(result[1]);
						Assert.IsNull(result[2]);
					}
					
					session.Delete(gavin);
					session.Delete(leonardo);
					session.Delete(johnDoe);
					session.Delete(courseA);
					session.Delete(courseB);

					t.Commit();
				}
			}
		}
	}
}