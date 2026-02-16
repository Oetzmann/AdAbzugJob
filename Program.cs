using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.DirectoryServices;

namespace AdAbzugJob
{
    /// <summary>
    /// Täglicher Job: AD-Abzug (alle Benutzer), Snapshot in azm_tool_ad_abzug_eintraege,
    /// Abgleich mit vorherigem Lauf, Einträge in azm_tool_ad_aenderungen.
    /// Ausführung z. B. per Windows-Aufgabenplanung.
    /// </summary>
    static class Program
    {
        private const string StatusNeu = "NEU";

        static int Main(string[] args)
        {
            try
            {
                string connStr = ConfigurationManager.ConnectionStrings["AZMTool"].ConnectionString;
                if (string.IsNullOrEmpty(connStr))
                {
                    Console.WriteLine("Fehler: ConnectionString 'AZMTool' in App.config fehlt.");
                    return 1;
                }

                DateTime abzugZeitpunkt = DateTime.UtcNow;

                Console.WriteLine("AD-Abzug starten: " + abzugZeitpunkt.ToString("yyyy-MM-dd HH:mm:ss") + " UTC");

                List<AdEintrag> eintraege = AdLesen();
                Console.WriteLine("AD-Einträge gelesen: " + eintraege.Count.ToString());

                if (eintraege.Count == 0)
                {
                    Console.WriteLine("Keine AD-Benutzer gefunden – Abbruch.");
                    return 0;
                }

                SnapshotSpeichern(connStr, abzugZeitpunkt, eintraege);
                Console.WriteLine("Snapshot in azm_tool_ad_abzug_eintraege geschrieben.");

                AbgleichDurchfuehren(connStr, abzugZeitpunkt, eintraege);
                Console.WriteLine("Abgleich durchgeführt, azm_tool_ad_aenderungen aktualisiert.");

                string workConnStr = null;
                var workCs = ConfigurationManager.ConnectionStrings["SPExpert_Work"];
                if (workCs != null && !string.IsNullOrEmpty(workCs.ConnectionString))
                    workConnStr = workCs.ConnectionString;
                if (!string.IsNullOrEmpty(workConnStr))
                {
                    FillMissingGuidsFromSnapshot(workConnStr, eintraege);
                    Console.WriteLine("Fehlende GUIDs (bei bekanntem ADUsername) ergänzt.");
                    SyncMetaFromSnapshot(workConnStr, eintraege);
                    Console.WriteLine("Meta-Sync (ADUsername/eMail bei bekannter GUID) durchgeführt.");
                }

                Console.WriteLine("AD-Abzug beendet.");
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Fehler: " + ex.Message);
                if (ex.StackTrace != null)
                    Console.WriteLine(ex.StackTrace);
                return 1;
            }
        }

        private static List<AdEintrag> AdLesen()
        {
            var list = new List<AdEintrag>();
            using (DirectoryEntry entry = new DirectoryEntry())
            {
                using (DirectorySearcher searcher = new DirectorySearcher(entry))
                {
                    searcher.Filter = "(&(objectCategory=person)(objectClass=user))";
                    searcher.SearchScope = SearchScope.Subtree;
                    searcher.PageSize = 1000;
                    searcher.PropertiesToLoad.Add("objectGuid");
                    searcher.PropertiesToLoad.Add("sAMAccountName");
                    searcher.PropertiesToLoad.Add("mail");
                    searcher.PropertiesToLoad.Add("displayName");
                    searcher.PropertiesToLoad.Add("company");
                    searcher.PropertiesToLoad.Add("department");

                    using (SearchResultCollection results = searcher.FindAll())
                    {
                        foreach (SearchResult result in results)
                        {
                            AdEintrag e = EintragAusResult(result);
                            if (e != null)
                                list.Add(e);
                        }
                    }
                }
            }
            return list;
        }

        private static AdEintrag EintragAusResult(SearchResult result)
        {
            if (result.Properties["objectGuid"] == null || result.Properties["objectGuid"].Count == 0)
                return null;
            byte[] guidBytes = (byte[])result.Properties["objectGuid"][0];
            Guid guid = new Guid(guidBytes);

            string username = GetString(result, "sAMAccountName");
            if (string.IsNullOrEmpty(username))
                return null;

            string mail = GetString(result, "mail");
            string nameVorname = GetString(result, "displayName");
            string firma = GetString(result, "company");
            string abteilung = GetString(result, "department");

            return new AdEintrag
            {
                AdObjectGuid = guid,
                Username = username,
                Mail = mail ?? "",
                NameVorname = nameVorname ?? "",
                Firma = firma ?? "",
                Abteilung = abteilung ?? ""
            };
        }

        private static string GetString(SearchResult result, string propertyName)
        {
            if (result.Properties[propertyName] == null || result.Properties[propertyName].Count == 0)
                return null;
            object v = result.Properties[propertyName][0];
            return (v == null) ? null : v.ToString().Trim();
        }

        private static void SnapshotSpeichern(string connStr, DateTime abzugZeitpunkt, List<AdEintrag> eintraege)
        {
            using (SqlConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                using (SqlTransaction trans = conn.BeginTransaction())
                {
                    try
                    {
                        // Alte Abzüge löschen, nur den letzten Snapshot behalten (für Abgleich „vorheriger Lauf“)
                        string sqlDelete = @"DELETE FROM [dbo].[azm_tool_ad_abzug_eintraege]
WHERE [AbzugZeitpunkt] NOT IN (SELECT TOP 1 [AbzugZeitpunkt] FROM [dbo].[azm_tool_ad_abzug_eintraege] ORDER BY [AbzugZeitpunkt] DESC)";
                        using (SqlCommand delCmd = new SqlCommand(sqlDelete, conn, trans))
                        {
                            delCmd.ExecuteNonQuery();
                        }

                        string sql = "INSERT INTO [dbo].[azm_tool_ad_abzug_eintraege] ([AbzugZeitpunkt],[AdObjectGuid],[Username],[Mail],[NameVorname],[Firma],[Abteilung]) VALUES (@AbzugZeitpunkt,@AdObjectGuid,@Username,@Mail,@NameVorname,@Firma,@Abteilung)";
                        using (SqlCommand cmd = new SqlCommand(sql, conn, trans))
                        {
                            cmd.Parameters.Add("@AbzugZeitpunkt", System.Data.SqlDbType.DateTime2);
                            cmd.Parameters.Add("@AdObjectGuid", System.Data.SqlDbType.UniqueIdentifier);
                            cmd.Parameters.Add("@Username", System.Data.SqlDbType.NVarChar, 128);
                            cmd.Parameters.Add("@Mail", System.Data.SqlDbType.NVarChar, 256);
                            cmd.Parameters.Add("@NameVorname", System.Data.SqlDbType.NVarChar, 200);
                            cmd.Parameters.Add("@Firma", System.Data.SqlDbType.NVarChar, 100);
                            cmd.Parameters.Add("@Abteilung", System.Data.SqlDbType.NVarChar, 100);

                            foreach (AdEintrag e in eintraege)
                            {
                                cmd.Parameters["@AbzugZeitpunkt"].Value = abzugZeitpunkt;
                                cmd.Parameters["@AdObjectGuid"].Value = e.AdObjectGuid;
                                cmd.Parameters["@Username"].Value = e.Username ?? (object)DBNull.Value;
                                cmd.Parameters["@Mail"].Value = string.IsNullOrEmpty(e.Mail) ? DBNull.Value : (object)e.Mail;
                                cmd.Parameters["@NameVorname"].Value = string.IsNullOrEmpty(e.NameVorname) ? DBNull.Value : (object)e.NameVorname;
                                cmd.Parameters["@Firma"].Value = string.IsNullOrEmpty(e.Firma) ? DBNull.Value : (object)e.Firma;
                                cmd.Parameters["@Abteilung"].Value = string.IsNullOrEmpty(e.Abteilung) ? DBNull.Value : (object)e.Abteilung;
                                cmd.ExecuteNonQuery();
                            }
                        }
                        trans.Commit();
                    }
                    catch
                    {
                        trans.Rollback();
                        throw;
                    }
                }
            }
        }

        private static void AbgleichDurchfuehren(string connStr, DateTime abzugZeitpunkt, List<AdEintrag> aktuelleEintraege)
        {
            DateTime? vorherigerAbzug = VorherigenAbzugZeitpunktLesen(connStr, abzugZeitpunkt);
            if (vorherigerAbzug == null)
            {
                return;
            }

            Dictionary<Guid, AdEintrag> vorherige = SnapshotLesen(connStr, vorherigerAbzug.Value);
            DateTime jetzt = DateTime.UtcNow;

            using (SqlConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                foreach (AdEintrag aktuell in aktuelleEintraege)
                {
                    AdEintrag vorher;
                    bool istNeu = !vorherige.TryGetValue(aktuell.AdObjectGuid, out vorher);
                    bool geaendert = !istNeu && (VorherWert(vorher.Username) != VorherWert(aktuell.Username) || VorherWert(vorher.Mail) != VorherWert(aktuell.Mail));

                    if (istNeu)
                    {
                        AenderungEinfuegen(conn, aktuell, null, jetzt);
                    }
                    else if (geaendert)
                    {
                        AenderungAktualisierenOderEinfuegen(conn, aktuell, vorher, jetzt);
                    }
                }
            }
        }

        private static string VorherWert(string s)
        {
            return string.IsNullOrEmpty(s) ? "" : s.Trim();
        }

        private static DateTime? VorherigenAbzugZeitpunktLesen(string connStr, DateTime aktuellerAbzug)
        {
            string sql = "SELECT MAX([AbzugZeitpunkt]) FROM [dbo].[azm_tool_ad_abzug_eintraege] WHERE [AbzugZeitpunkt] < @Aktuell";
            using (SqlConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@Aktuell", aktuellerAbzug);
                    object o = cmd.ExecuteScalar();
                    if (o == null || o == DBNull.Value)
                        return null;
                    return (DateTime)o;
                }
            }
        }

        private static Dictionary<Guid, AdEintrag> SnapshotLesen(string connStr, DateTime abzugZeitpunkt)
        {
            var dict = new Dictionary<Guid, AdEintrag>();
            string sql = "SELECT [AdObjectGuid],[Username],[Mail],[NameVorname],[Firma],[Abteilung] FROM [dbo].[azm_tool_ad_abzug_eintraege] WHERE [AbzugZeitpunkt] = @AbzugZeitpunkt";
            using (SqlConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@AbzugZeitpunkt", abzugZeitpunkt);
                    using (SqlDataReader r = cmd.ExecuteReader())
                    {
                        while (r.Read())
                        {
                            var e = new AdEintrag
                            {
                                AdObjectGuid = r.GetGuid(0),
                                Username = r.IsDBNull(1) ? "" : r.GetString(1),
                                Mail = r.IsDBNull(2) ? "" : r.GetString(2),
                                NameVorname = r.IsDBNull(3) ? "" : r.GetString(3),
                                Firma = r.IsDBNull(4) ? "" : r.GetString(4),
                                Abteilung = r.IsDBNull(5) ? "" : r.GetString(5)
                            };
                            dict[e.AdObjectGuid] = e;
                        }
                    }
                }
            }
            return dict;
        }

        private static void AenderungEinfuegen(SqlConnection conn, AdEintrag aktuell, AdEintrag vorher, DateTime jetzt)
        {
            string alterUsername = vorher != null ? vorher.Username : null;
            string alteMail = vorher != null ? vorher.Mail : null;
            string sql = "INSERT INTO [dbo].[azm_tool_ad_aenderungen] ([AdObjectGuid],[Status],[ErkanntAm],[MaNr],[NameVorname],[Firma],[Abteilung],[AlterAdUsername],[NeuerAdUsername],[AlteMail],[NeueMail],[CreatedAt],[UpdatedAt]) VALUES (@AdObjectGuid,@Status,@ErkanntAm,NULL,@NameVorname,@Firma,@Abteilung,@AlterAdUsername,@NeuerAdUsername,@AlteMail,@NeueMail,@Jetzt,@Jetzt)";
            using (SqlCommand cmd = new SqlCommand(sql, conn))
            {
                cmd.Parameters.AddWithValue("@AdObjectGuid", aktuell.AdObjectGuid);
                cmd.Parameters.AddWithValue("@Status", StatusNeu);
                cmd.Parameters.AddWithValue("@ErkanntAm", jetzt);
                cmd.Parameters.AddWithValue("@NameVorname", aktuell.NameVorname ?? "");
                cmd.Parameters.AddWithValue("@Firma", string.IsNullOrEmpty(aktuell.Firma) ? (object)DBNull.Value : aktuell.Firma);
                cmd.Parameters.AddWithValue("@Abteilung", string.IsNullOrEmpty(aktuell.Abteilung) ? (object)DBNull.Value : aktuell.Abteilung);
                cmd.Parameters.AddWithValue("@AlterAdUsername", string.IsNullOrEmpty(alterUsername) ? (object)DBNull.Value : alterUsername);
                cmd.Parameters.AddWithValue("@NeuerAdUsername", aktuell.Username ?? "");
                cmd.Parameters.AddWithValue("@AlteMail", string.IsNullOrEmpty(alteMail) ? (object)DBNull.Value : alteMail);
                cmd.Parameters.AddWithValue("@NeueMail", aktuell.Mail ?? "");
                cmd.Parameters.AddWithValue("@Jetzt", jetzt);
                cmd.ExecuteNonQuery();
            }
        }

        private static void AenderungAktualisierenOderEinfuegen(SqlConnection conn, AdEintrag aktuell, AdEintrag vorher, DateTime jetzt)
        {
            string sqlSelect = "SELECT [Status] FROM [dbo].[azm_tool_ad_aenderungen] WHERE [AdObjectGuid] = @AdObjectGuid";
            using (SqlCommand cmd = new SqlCommand(sqlSelect, conn))
            {
                cmd.Parameters.AddWithValue("@AdObjectGuid", aktuell.AdObjectGuid);
                object o = cmd.ExecuteScalar();
                if (o != null && o != DBNull.Value)
                {
                    string status = o.ToString();
                    string sqlUpdate = "UPDATE [dbo].[azm_tool_ad_aenderungen] SET [Status]=@Status,[ErkanntAm]=@ErkanntAm,[NameVorname]=@NameVorname,[Firma]=@Firma,[Abteilung]=@Abteilung,[AlterAdUsername]=@AlterAdUsername,[NeuerAdUsername]=@NeuerAdUsername,[AlteMail]=@AlteMail,[NeueMail]=@NeueMail,[UpdatedAt]=@Jetzt WHERE [AdObjectGuid]=@AdObjectGuid";
                    using (SqlCommand upd = new SqlCommand(sqlUpdate, conn))
                    {
                        upd.Parameters.AddWithValue("@Status", StatusNeu);
                        upd.Parameters.AddWithValue("@ErkanntAm", jetzt);
                        upd.Parameters.AddWithValue("@NameVorname", aktuell.NameVorname ?? "");
                        upd.Parameters.AddWithValue("@Firma", string.IsNullOrEmpty(aktuell.Firma) ? (object)DBNull.Value : aktuell.Firma);
                        upd.Parameters.AddWithValue("@Abteilung", string.IsNullOrEmpty(aktuell.Abteilung) ? (object)DBNull.Value : aktuell.Abteilung);
                        upd.Parameters.AddWithValue("@AlterAdUsername", string.IsNullOrEmpty(vorher.Username) ? (object)DBNull.Value : vorher.Username);
                        upd.Parameters.AddWithValue("@NeuerAdUsername", aktuell.Username ?? "");
                        upd.Parameters.AddWithValue("@AlteMail", string.IsNullOrEmpty(vorher.Mail) ? (object)DBNull.Value : vorher.Mail);
                        upd.Parameters.AddWithValue("@NeueMail", aktuell.Mail ?? "");
                        upd.Parameters.AddWithValue("@Jetzt", jetzt);
                        upd.Parameters.AddWithValue("@AdObjectGuid", aktuell.AdObjectGuid);
                        upd.ExecuteNonQuery();
                    }
                }
                else
                {
                    AenderungEinfuegen(conn, aktuell, vorher, jetzt);
                }
            }
        }

        /// <summary>
        /// Findet alle KEY mit gesetztem ADUsername aber ohne ADObjectGuid; sucht den Benutzer im aktuellen AD-Snapshot
        /// und schreibt bei Treffer die GUID in MADB_Meta (meta_name='ADObjectGuid').
        /// </summary>
        private static void FillMissingGuidsFromSnapshot(string workConnStr, List<AdEintrag> eintraege)
        {
            var usernameToEintrag = new Dictionary<string, AdEintrag>(StringComparer.OrdinalIgnoreCase);
            foreach (AdEintrag e in eintraege)
            {
                if (!string.IsNullOrEmpty(e.Username))
                    usernameToEintrag[e.Username] = e;
            }

            string sqlSelect = @"SELECT m.[KEY], LTRIM(RTRIM(ISNULL(m.meta_value,''))) AS AdUsername
FROM [dbo].[MADB_Meta] m
WHERE m.meta_name = 'ADUsername' AND LTRIM(RTRIM(ISNULL(m.meta_value,''))) <> ''
AND NOT EXISTS (SELECT 1 FROM [dbo].[MADB_Meta] g WHERE g.[KEY] = m.[KEY] AND g.meta_name = 'ADObjectGuid' AND LTRIM(RTRIM(ISNULL(g.meta_value,''))) <> '')";
            string sqlMerge = "MERGE [dbo].[MADB_Meta] AS t USING (SELECT @Key AS [KEY], @meta_name AS meta_name, @meta_value AS meta_value) AS s ON t.[KEY] = s.[KEY] AND t.meta_name = s.meta_name WHEN MATCHED THEN UPDATE SET meta_value = s.meta_value WHEN NOT MATCHED THEN INSERT ([KEY], meta_name, meta_value) VALUES (s.[KEY], s.meta_name, s.meta_value);";

            var toUpdate = new List<KeyUsernameRow>();
            using (SqlConnection conn = new SqlConnection(workConnStr))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(sqlSelect, conn))
                {
                    using (SqlDataReader r = cmd.ExecuteReader())
                    {
                        while (r.Read())
                        {
                            string key = r.IsDBNull(0) ? null : r.GetString(0);
                            string adUsername = r.IsDBNull(1) ? null : r.GetString(1);
                            if (string.IsNullOrEmpty(key) || string.IsNullOrEmpty(adUsername))
                                continue;
                            AdEintrag eintrag;
                            if (!usernameToEintrag.TryGetValue(adUsername, out eintrag))
                                continue;
                            toUpdate.Add(new KeyUsernameRow { Key = key, AdUsername = adUsername, Eintrag = eintrag });
                        }
                    }
                }
                foreach (KeyUsernameRow row in toUpdate)
                {
                    using (SqlCommand mergeCmd = new SqlCommand(sqlMerge, conn))
                    {
                        mergeCmd.Parameters.AddWithValue("@Key", row.Key);
                        mergeCmd.Parameters.AddWithValue("@meta_name", "ADObjectGuid");
                        mergeCmd.Parameters.AddWithValue("@meta_value", row.Eintrag.AdObjectGuid.ToString());
                        mergeCmd.ExecuteNonQuery();
                    }
                }
            }
        }

        /// <summary>
        /// Liest aus SPExpert_Work.MADB_Meta alle KEY mit meta_name='ADObjectGuid', findet den Eintrag im aktuellen Snapshot
        /// und schreibt für diesen KEY ADUsername und eMail in MADB_Meta (MERGE).
        /// </summary>
        private static void SyncMetaFromSnapshot(string workConnStr, List<AdEintrag> eintraege)
        {
            var guidToEintrag = new Dictionary<Guid, AdEintrag>();
            foreach (AdEintrag e in eintraege)
                guidToEintrag[e.AdObjectGuid] = e;

            string sqlSelect = "SELECT [KEY], LTRIM(RTRIM(ISNULL(meta_value,''))) AS meta_value FROM [dbo].[MADB_Meta] WHERE meta_name = 'ADObjectGuid' AND LTRIM(RTRIM(ISNULL(meta_value,''))) <> ''";
            string sqlMerge = "MERGE [dbo].[MADB_Meta] AS t USING (SELECT @Key AS [KEY], @meta_name AS meta_name, @meta_value AS meta_value) AS s ON t.[KEY] = s.[KEY] AND t.meta_name = s.meta_name WHEN MATCHED THEN UPDATE SET meta_value = s.meta_value WHEN NOT MATCHED THEN INSERT ([KEY], meta_name, meta_value) VALUES (s.[KEY], s.meta_name, s.meta_value);";

            var toSync = new List<KeyGuidRow>();
            using (SqlConnection conn = new SqlConnection(workConnStr))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(sqlSelect, conn))
                {
                    using (SqlDataReader r = cmd.ExecuteReader())
                    {
                        while (r.Read())
                        {
                            string key = r.IsDBNull(0) ? null : r.GetString(0);
                            string guidStr = r.IsDBNull(1) ? null : r.GetString(1);
                            if (string.IsNullOrEmpty(key) || string.IsNullOrEmpty(guidStr))
                                continue;
                            Guid g;
                            if (!Guid.TryParse(guidStr, out g))
                                continue;
                            AdEintrag eintrag;
                            if (!guidToEintrag.TryGetValue(g, out eintrag))
                                continue;
                            toSync.Add(new KeyGuidRow { Key = key, Eintrag = eintrag });
                        }
                    }
                }
                foreach (KeyGuidRow row in toSync)
                {
                    using (SqlCommand mergeCmd = new SqlCommand(sqlMerge, conn))
                    {
                        mergeCmd.Parameters.AddWithValue("@Key", row.Key);
                        mergeCmd.Parameters.AddWithValue("@meta_name", "ADUsername");
                        mergeCmd.Parameters.AddWithValue("@meta_value", row.Eintrag.Username ?? "");
                        mergeCmd.ExecuteNonQuery();
                    }
                    using (SqlCommand mergeCmd = new SqlCommand(sqlMerge, conn))
                    {
                        mergeCmd.Parameters.AddWithValue("@Key", row.Key);
                        mergeCmd.Parameters.AddWithValue("@meta_name", "eMail");
                        mergeCmd.Parameters.AddWithValue("@meta_value", row.Eintrag.Mail ?? "");
                        mergeCmd.ExecuteNonQuery();
                    }
                }
            }
        }

        private class KeyUsernameRow
        {
            public string Key { get; set; }
            public string AdUsername { get; set; }
            public AdEintrag Eintrag { get; set; }
        }

        private class KeyGuidRow
        {
            public string Key { get; set; }
            public AdEintrag Eintrag { get; set; }
        }

        private class AdEintrag
        {
            public Guid AdObjectGuid { get; set; }
            public string Username { get; set; }
            public string Mail { get; set; }
            public string NameVorname { get; set; }
            public string Firma { get; set; }
            public string Abteilung { get; set; }
        }
    }
}
