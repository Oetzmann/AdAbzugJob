# AdAbzugJob

Konsolenprogramm für den **täglichen AD-Abzug**: liest alle AD-Benutzer, schreibt einen Snapshot in `azm_tool_ad_abzug_eintraege`, vergleicht mit dem vorherigen Lauf und pflegt `azm_tool_ad_aenderungen`.

## Voraussetzungen

- .NET Framework 4.7.2
- Zugriff auf die AZMtool-Datenbank (gleiche DB wie die Webanwendung)
- Laufender Zugriff auf Active Directory (Domain-Join oder konfigurierter LDAP-Pfad)

## Konfiguration

**App.config:** Connection String `AZMTool` anpassen (Datenquelle, Initial Catalog = z. B. `SPExpert_AZMTool`). Optional: LDAP-Pfad für AD (derzeit wird der Standard-Domain-Kontext verwendet).

## Ausführung

- Manuell: `AdAbzugJob.exe` im Ordner `bin\Debug` oder `bin\Release`.
- Täglich: Windows-Aufgabenplanung (Task Scheduler) einrichten, z. B. einmal täglich um 6:00 Uhr, Aktion „Programm starten“ = Pfad zu `AdAbzugJob.exe`, Starten in = Verzeichnis der exe.

## Datenbank

Die Tabellen müssen vor dem ersten Lauf existieren. Skript: `azmtool\Sql\25_azm_tool_ad_aenderungen_Create.sql` in der AZMtool-Datenbank ausführen (Katalog ggf. anpassen, z. B. `USE [SPExpert_AZMTool]` statt `USE [DEV]`).

## Ablauf

1. Liest alle Benutzer aus dem AD (objectCategory=person, objectClass=user), Attribute: objectGuid, sAMAccountName, mail, displayName, company, department.
2. Schreibt jeden Eintrag in `azm_tool_ad_abzug_eintraege` mit aktuellem `AbzugZeitpunkt`.
3. Ermittelt den vorherigen `AbzugZeitpunkt` (MAX wo < aktuell).
4. Vergleicht aktuellen Snapshot mit vorherigem: neue GUIDs = neue Benutzer (INSERT in `azm_tool_ad_aenderungen`, Status NEU); geänderte sAMAccountName/Mail = UPDATE bzw. INSERT (Status NEU).
