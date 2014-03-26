using System;
using System.Data;using System.Data.Common;

namespace NHibernate.SqlTypes
{
	[Serializable]
	public class XmlSqlType : SqlType
	{
		public XmlSqlType()
			: base(DbType.Xml)
		{
		}

		public XmlSqlType(int length) : base(DbType.Xml, length)
		{
		}
	}
}