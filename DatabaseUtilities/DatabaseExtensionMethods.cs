using System;
using System.ComponentModel;
using System.Data.Common;

namespace NatWallbank.DatabaseUtilities
{
    /// <summary>
    /// Provides extension methods for System.Data classes.
    /// </summary>
    public static class DatabaseExtensions
    {
        /// <summary>
        /// Tries to read a column value from a data reader and return the typed value.
        /// </summary>
        /// <remarks>If the specified column is not found or the types do not match, returns default value.</remarks>
        /// <typeparam name="TField">The type of the field that we are reading.</typeparam>
        /// <param name="reader">The data reader that should contain the column value.</param>
        /// <param name="name">The name of the column to read.</param>
        /// <returns>A value of type TField.</returns>
        public static TField Get<TField>(this DbDataReader reader, string name)
        {
            var value = reader[name];
            // check for null and return default
            if (value == null || value == DBNull.Value)
                return default(TField);

            // direct cast if we can...
            var returnType = typeof(TField);
            if (value.GetType() == returnType)
                return (TField)value;

            // nullable types require different container
            if (returnType.IsGenericType && returnType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                var converter = new NullableConverter(returnType);
                if (converter.CanConvertFrom(value.GetType()))
                    return (TField)converter.ConvertFrom(value);
            }
            else
                return (TField)Convert.ChangeType(value, typeof(TField));

            // We tried...we failed...
            throw new InvalidOperationException(
                $"Unable to convert from database field ({value.GetType().FullName}) to object ({returnType.FullName})");
        }
    }
}
