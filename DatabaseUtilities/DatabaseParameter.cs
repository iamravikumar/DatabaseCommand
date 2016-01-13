using System.Data;

namespace NatWallbank.DatabaseUtilities
{
    public class DatabaseParameter
    {
        public DatabaseParameter(string name = null, object value = null,
                                 ParameterDirection direction = ParameterDirection.Input, DbType? dbType = null,
                                 int? size = null)
        {
            Name = name;
            Value = value;
            Direction = direction;
            DbType = dbType;
            Size = size;
        }

        public string Name { get; set; }
        public object Value { get; set; }
        public ParameterDirection Direction { get; set; }
        public DbType? DbType { get; set; }
        public int? Size { get; set; }
    }
}
