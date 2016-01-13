using System;
using System.Configuration;
using System.Data.Common;
using System.Threading;
using DatabaseCommandExampleConsole.Models;
using NatWallbank.DatabaseUtilities;

namespace DatabaseCommandExampleConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            var settings = ConfigurationManager.ConnectionStrings["TestDB"];
            var cmd = new DatabaseCommand(settings.ProviderName, settings.ConnectionString, "[dbo].[GetContacts]");
            var allContacts = cmd.GetRecords(MapContact);

            Console.WriteLine("Listing all contacts...");
            foreach (var contact in allContacts)
            {
                OutputContact(contact);
            }

            while (true)
            {
                Console.WriteLine();
                Console.Write("Enter a contact ID (ctrl-c to exit): ");
                var id = Console.ReadLine();

                // try and load the contact...
                cmd.Parameters = new { id };
                var result = cmd.GetRecord(MapContact);
                if (result == null)
                    Console.WriteLine($"Contact with ID of {id} not found in the database!");
                else
                    OutputContact(cmd.GetRecord(MapContact));
                Thread.Sleep(1000);
            }
        }

        private static Contact MapContact(DbDataReader rdr)
        {
            return new Contact()
            {
                Id = rdr.Get<int>("ContactId"),
                Surname = rdr.Get<string>("Surname"),
                Forenames = rdr.Get<string>("Forenames")
            };
        }

        private static void OutputContact(Contact contact)
        {
            Console.WriteLine($"ID: {contact.Id} / Surname: {contact.Surname} / Forenames: {contact.Forenames}");
        }
    }
}
